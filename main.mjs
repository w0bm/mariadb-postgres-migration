import my from "promise-mysql";
import pg from "pg-promise";
import { default as cfg } from "./config.json";

//create db connections
const pgdb = (new pg())(cfg.postgres);
const mydb = my.createPool(cfg.mysql);

//function for printing an objects (mainly errors) in a readable way
const pretty_print = obj => console.log(JSON.stringify(obj, null, 2));

//prints "inserting <table>..." for multiple tables
const insert_msg = arr => console.log(arr.map(t => "inserting " + t + "...").join("\n"));

//select tags and videos
const select_videos_tags = "SELECT v.id, tags.name, tags.normalized FROM videos as v JOIN taggable_taggables as vt ON v.id = vt.taggable_id JOIN taggable_tags as tags ON tags.tag_id = vt.tag_id";

//add tag to video
const update_video_tags = "UPDATE videos SET tags = array_append(tags, $1) WHERE videos.id = $2";

//insert queries for shortening insert functions
const insert_users = "INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)";
const insert_msgs = "INSERT INTO messages(id, from_user, to_user, title, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)";
const insert_videos = "INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash) VALUES($1, $2, $3, $4, $5, $6)";
const insert_comments = "INSERT INTO comments(id, user_id, video_id, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)";
const insert_tags = "INSERT INTO tags(normalized, tag) VALUES($1, $2) ON CONFLICT DO NOTHING";

//maps for shortening query functions
const map_user = r => [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)];
const map_msg = r => [r.id, r.from, r.to, r.subject, r.content, r.created_at, r.updated_at, r.deleted_at];
const map_video = r => [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash];
const map_comment = r => [r.id, r.user_id, r.video_id, r.content, r.created_at, r.updated_at, r.deleted_at];
const map_tags = r => [r.normalized.substring(0, 30), r.name.substring(0, 30)];
const map_tags_video = r => [r.name.substring(0, 30), r.id];

//helper function to avoid redundant code
const set_auto_increment_and_cluster = (promise, table) => promise.then(result => {
    console.log(result.length + " " + table + " inserted, adjusting auto increment value...");
    //set auto increment because ids may be missing in between in origin table
    return pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
        .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
            .then(() => console.log("adjusted auto increment for " + table + " table"))
        );
});

//copy users
insert_msg(["users"]);
set_auto_increment_and_cluster(mydb.query("SELECT * FROM users").then(rows => Promise.all(rows.map(r => pgdb.none(insert_users, map_user(r))))), "users")
    //copy videos and messages
    .then(() => {
        insert_msg(["videos", "messages"]);
        return Promise.all([
            set_auto_increment_and_cluster(mydb.query("SELECT * FROM messages").then(rows => Promise.all(rows.map(r => pgdb.none(insert_msgs, map_msg(r))))), "messages"),
            set_auto_increment_and_cluster(mydb.query("SELECT * FROM videos").then(rows => Promise.all(rows.map(r => pgdb.none(insert_videos, map_video(r))))), "videos")
                .then(() => Promise.all([
                    //copy comments
                    (() => {
                        insert_msg(["comments"]);
                        return set_auto_increment_and_cluster(mydb.query("SELECT * FROM comments").then(rows => Promise.all(rows.map(r => pgdb.none(insert_comments, map_comment(r))))), "comments");
                    })(),
                    //tag videos
                    (() => {
                        console.log("tagging videos...");
                        return mydb.query(select_videos_tags)
                            .then(rows => Promise.all(rows.map(r => pgdb.none(insert_tags, map_tags(r)).then(pgdb.none(update_video_tags, map_tags_video(r))))).then(() => console.log("all videos tagged")));
                    })()
                ]))
        ]);
    })
    //cluster all tables because items are added asynchronously and thus are not in order
    .then(() => Promise.all(["users", "videos", "comments", "messages"].map(table => pgdb.none("CLUSTER " + table + " USING " + table + "_pkey").then(() => console.log("successfully clustered " + table + " table")))))
    .catch(pretty_print) // error handling
    .then(() => { // close connections
        mydb.end();
        pgdb.$pool.end();
    });

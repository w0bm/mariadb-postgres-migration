import my from "promise-mysql";
import pg from "pg-promise";
import { default as cfg } from "./config.json";

//create db connections
const pgdb = (new pg())(cfg.postgres)
    , mydb = my.createPool(cfg.mysql);

//function for printing an objects (mainly errors) in a readable way
const pretty_print = obj => console.log(JSON.stringify(obj, null, 2));

//prints "inserting <table>..." for multiple tables
const insert_msg = arr => console.log(arr.map(t => "inserting " + t + "...").join("\n"));

//select videos with their tags
const select_videos_tags = `SELECT v.*, GROUP_CONCAT(DISTINCT t.normalized) as tags
                            FROM videos v, taggable_taggables tt, taggable_tags t
                            WHERE v.id = tt.taggable_id AND tt.tag_id = t.tag_id
                            GROUP BY v.id`;

//insert queries for shortening insert functions
const insert_users = "INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    , insert_msgs = "INSERT INTO messages(id, from_user, to_user, title, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
    , insert_videos = "INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash, tags) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_comments = "INSERT INTO comments(id, user_id, video_id, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_tags = "INSERT INTO tags(normalized, tag) VALUES($1, $2)";

//maps for shortening query functions
const map_user = r => [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)]
    , map_msg = r => [r.id, r.from, r.to, r.subject, r.content, r.created_at, r.updated_at, r.deleted_at]
    , map_video = r => [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash, r.tags.split(",").map(t => t.substring(0, 30))]
    , map_comment = r => [r.id, r.user_id, r.video_id, r.content, r.created_at, r.updated_at, r.deleted_at];

//helper function to avoid redundant code
const set_auto_increment_and_cluster = (promise, table) => promise.then(result => {
    console.log(result.length + " " + table + " inserted, adjusting auto increment value...");
    //set auto increment because ids may be missing in between in origin table
    return pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
        .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
            .then(() => {
                console.log("adjusted auto increment for " + table + " table, clustering table");
                //cluster tables because items are added asynchronously and thus are not in order
                return pgdb.none("CLUSTER " + table + " USING " + table + "_pkey")
                    .then(() => console.log("successfully clustered " + table + " table"));
            })
        );
});

console.time("done! elapsed time");
//copy users
insert_msg(["users"]);
set_auto_increment_and_cluster(mydb.query("SELECT * FROM users").then(rows => Promise.all(rows.map(r => pgdb.none(insert_users, map_user(r))))), "users")
    //copy videos and messages
    .then(() => {
        insert_msg(["videos", "messages"]);
        return Promise.all([
            set_auto_increment_and_cluster(mydb.query("SELECT * FROM messages").then(rows => Promise.all(rows.map(r => pgdb.none(insert_msgs, map_msg(r))))), "messages")
          , set_auto_increment_and_cluster(mydb.query(select_videos_tags).then(rows => Promise.all(rows.map(r => pgdb.none(insert_videos, map_video(r))))), "videos")
                .then(() => {
                    //copy comments and tags
                    insert_msg(["comments", "tags"]);
                    return Promise.all([
                        set_auto_increment_and_cluster(mydb.query("SELECT * FROM comments").then(rows => Promise.all(rows.map(r => pgdb.none(insert_comments, map_comment(r))))), "comments"),
                        pgdb.any("SELECT DISTINCT UNNEST(tags) FROM videos")
                            .then(rows => Promise.all(rows.map(r => pgdb.none(insert_tags, [r.unnest, r.unnest]).catch(pretty_print))).then(result => console.log(result.length + " tags inserted")))
                    ]);
                })
        ]);
    })
    .catch(pretty_print) // error handling
    .then(() => { // close connections
        console.log("closing db connections...");
        mydb.end();
        pgdb.$pool.end();
        console.timeEnd("done! elapsed time");
    });

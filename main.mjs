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


//helper function to avoid redundant code
const set_auto_increment_and_cluster = (promise, table) => promise.then(result => {
    console.log(result.length + " " + table + " inserted, adjusting auto increment value...");
    //set auto increment because ids may be missing in between in origin table
    return pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
        .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
            .then(() => {
                console.log("adjusted auto increment for " + table + " table, clustering table...");
                //cluster table because items are added asynchronously and thus are not in order
                return pgdb.none("CLUSTER " + table + " USING " + table + "_pkey")
                    .then(() => console.log("successfully clustered " + table + " table"))
            })
        );
});

//copy users
insert_msg(["users"]);
set_auto_increment_and_cluster(mydb.query("SELECT * FROM users")
    .then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)", [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)])))), "users")
//copy videos and messages
.then(() => {
    insert_msg(["videos", "messages"]);
    return Promise.all([
        set_auto_increment_and_cluster(mydb.query("SELECT * FROM messages")
            .then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO messages(id, from_user, to_user, title, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)", [r.id, r.from, r.to, r.subject, r.content, r.created_at, r.updated_at, r.deleted_at])))), "messages")
      , set_auto_increment_and_cluster(mydb.query("SELECT * FROM videos")
            .then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash) VALUES($1, $2, $3, $4, $5, $6)", [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash])))), "videos")
            //copy comments
            .then(() => {
                insert_msg(["comments"]);
                return set_auto_increment_and_cluster(mydb.query("SELECT * FROM comments")
                    .then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO comments(id, user_id, video_id, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)", [r.id, r.user_id, r.video_id, r.content, r.created_at, r.updated_at, r.deleted_at])))), "comments");
            })
    ]);
})
.catch(pretty_print) // error handling
.then(() => { // close connections
    mydb.end();
    pgdb.$pool.end();
});

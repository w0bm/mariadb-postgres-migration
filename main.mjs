import my from "promise-mysql";
import pg from "pg-promise";
import { default as cfg } from "./config.json";

//create db connections
const pgdb = (new pg())(cfg.postgres);
const mydb = my.createPool(cfg.mysql);

//function for printing an objects (mainly errors) in a readable way
const pretty_print = obj => console.log(JSON.stringify(obj, null, 2));

//helper function to avoid redundant code
function set_auto_increment_and_cluster(promise, table) {
    return promise.then(result => new Promise((res, rej) => {
        console.log(result.length + " " + table + " inserted, adjusting auto increment value...");
        //set auto increment because ids may be missing in between in origin table
        pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
            .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
                             .then(() => {
                                 console.log("adjusted auto increment for " + table + " table, clustering table...");
                                 //cluster table because items are added asynchronously and thus are not in order
                                 pgdb.none("CLUSTER " + table + " USING " + table + "_pkey")
                                     .then(() => {
                                         console.log("successfully clustered " + table + " table");
                                         res();
                                     })
                                     .catch(err => rej(err));
                             })
                             .catch(err => rej(err))
            )
            .catch(err => rej(err));
    }));
}

console.log(["users", "videos"].map(t => "inserting " + t + "...").join("\n"));
Promise.all([

    //copy users
    set_auto_increment_and_cluster(mydb.query("SELECT * FROM users").then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)", [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)])))), "users"),

    //copy videos
    set_auto_increment_and_cluster(mydb.query("SELECT * FROM videos").then(rows => Promise.all(rows.map(r => pgdb.none("INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash) VALUES($1, $2, $3, $4, $5, $6)", [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash])))), "videos")

])
.catch(err => pretty_print(err))
.then(() => {
    mydb.end();
    pgdb.$pool.end();
});

import my from "promise-mysql";
import pg from "pg-promise";
import { default as cfg } from "./config.json";

//create db connections
const pgdb = (new pg())(cfg.postgres)
    , mydb = my.createPool(cfg.mysql);

//select helpers
const select_full = table => "SELECT * FROM " + table;
const select_videos_tags = `SELECT v.*, GROUP_CONCAT(DISTINCT t.normalized) as tags
                            FROM videos v, taggable_taggables tt, taggable_tags t
                            WHERE v.id = tt.taggable_id AND tt.tag_id = t.tag_id
                            GROUP BY v.id`;

//queries to shorten insertions
const insert_users = "INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    , insert_msgs = "INSERT INTO messages(id, from_user, to_user, title, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
    , insert_videos = "INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash, tags) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_comments = "INSERT INTO comments(id, user_id, video_id, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_tags = "INSERT INTO tags(normalized, tag) VALUES($1, $2)"
    , insert_playlists = "INSERT INTO playlist_video(playlist_id, video_id, created_at) VALUES($1, $2, $3)";

//maps to shorten insertions
const map_user = r => [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)]
    , map_msg = r => [r.id, r.from, r.to, r.subject, r.content, r.created_at, r.updated_at, r.deleted_at]
    , map_video = r => [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash, r.tags.split(",").map(t => t.substring(0, 30))]
    , map_comment = r => [r.id, r.user_id, r.video_id, r.content, r.created_at, r.updated_at, r.deleted_at]
    , map_tags = r => [r.unnest, r.unnest];

//prints "inserting <table>..."
const log_insert_start = arr => console.log(arr.map(t => "inserting " + t + "...").join("\n"));

//prints "<count> <type> inserted"
const log_insert_done = (res, type) => console.log(`${res.length} ${type} inserted`);

//helper function for basic table copying
const copy_table = (select_query, insert_query, mapping) => mydb.query(select_query)
    .then(rows => Promise.all(rows.map(r => pgdb.none(insert_query, mapping(r)))));

console.time("done! elapsed time");
//copy users
log_insert_start(["users"]);
copy_table(select_full("users"), insert_users, map_user)
    .then(res => log_insert_done(res, "users"))
    //copy videos and messages
    .then(() => {
        log_insert_start(["videos", "messages"]);
        return Promise.all([
            copy_table(select_full("messages"), insert_msgs, map_msg)
                .then(res => log_insert_done(res, "messages")),
            copy_table(select_videos_tags, insert_videos, map_video)
                .then(res => log_insert_done(res, "videos"))
        ])
        .then(() => {
            //copy comments and tags
            log_insert_start(["comments", "tags"]);
            return Promise.all([
                copy_table(select_full("comments"), insert_comments, map_comment)
                    .then(res => log_insert_done(res, "comments")),
                pgdb.any("SELECT DISTINCT UNNEST(tags) FROM videos")
                    .then(rows => Promise.all(rows.map(r => pgdb.none(insert_tags, map_tags(r))))
                        .then(res => log_insert_done(res, "tags"))
                    )
            ]);
        })
    })
    //fill playlists
    .then(() => {
        //helper function for insertions
        const fill_playlists = (select_query, playlist_title, insert_mapping) => mydb.query(select_query)
            .then(videos => {
                log_insert_start([playlist_title.toLowerCase() + " into playlist_video"]);
                return pgdb.any("SELECT id, user_id FROM playlists WHERE title = $1", [playlist_title])
                    .then(playlists => {
                        let plMap = new Map(playlists.map(rel => [rel.user_id, rel.id].map(str => parseInt(str))));
                        return Promise.all(videos.map(v => pgdb.none(insert_playlists, insert_mapping(plMap, v))))
                            .then(res => log_insert_done(res, playlist_title.toLowerCase()));
                    })
            });
        const uploads_mapping = (m, v) => [m.get(v.user_id), v.id, v.created_at];
        const favorites_mapping = (m, v) => [m.get(v.user_id), v.video_id, v.created_at];
        return Promise.all([
            fill_playlists("SELECT id, user_id, created_at FROM videos", "Uploads", uploads_mapping),
            fill_playlists("SELECT user_id, video_id, created_at FROM favorites", "Favorites", favorites_mapping)
        ]);
    })
    //set auto increment because ids may be missing in between in origin table
    .then(() => Promise.all(["comments", "messages", "users", "videos"]
        .map(table => {
            console.log("adjusting auto increment for " + table + " table");
            return pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
                .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
                    .then(() => console.log("adjusted auto increment for " + table + " table"))
                );
        })
    ))
    .then(() => {
        //cluster all tables because items are added asynchronously and thus are not in order
        console.log("clustering all tables...");
        return Promise.all(["comments", "messages", "playlist_video", "playlists", "users", "videos"]
            .map(table => pgdb.none("CLUSTER " + table + " USING " + table + "_pkey")
                .then(() => console.log("successfully clustered " + table + " table"))
            )
        );
    })
    .catch(console.log) // error handling
    .then(() => { // close connections
        console.log("closing db connections...");
        mydb.end();
        pgdb.$pool.end();
        console.timeEnd("done! elapsed time");
    });

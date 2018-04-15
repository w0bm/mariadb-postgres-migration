import my from "promise-mysql";
import pg from "pg-promise";
import { default as cfg } from "./config.json";
import normalize_tags from "./normalize_tags";

//create db connections
const pgdb = (new pg())(cfg.postgres)
    , mydb = my.createPool(cfg.mysql);

//select helpers
const select_full = table => "SELECT * FROM " + table
    , select_videos_tags = `SELECT v.*, GROUP_CONCAT(DISTINCT t.name SEPARATOR '${cfg.tag_select_separator}') as tags
                            FROM videos v, taggable_taggables tt, taggable_tags t
                            WHERE v.id = tt.taggable_id AND tt.tag_id = t.tag_id
                            GROUP BY v.id`;

//queries to shorten insertions
const insert_users = "INSERT INTO users(id, username, password, created_at, updated_at, deleted_at, banned, banreason, filters) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    , insert_msgs = "INSERT INTO messages(id, from_user, to_user, title, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7, $8)"
    , insert_videos = "INSERT INTO videos(id, file, created_at, updated_at, deleted_at, hash, tags) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_comments = "INSERT INTO comments(id, user_id, video_id, content, created_at, updated_at, deleted_at) VALUES($1, $2, $3, $4, $5, $6, $7)"
    , insert_tags = "INSERT INTO tags(normalized, tag) VALUES($1, $2) ON CONFLICT DO NOTHING"
    , insert_playlists = "INSERT INTO playlist_video(playlist_id, video_id, created_at) VALUES($1, $2, $3)";

//maps to shorten insertions
const map_user = r => [r.id, r.username, cfg.password_placeholder, r.created_at, r.updated_at, null, r.banend, r.banreason, JSON.parse(r.categories)]
    , map_msg = r => [r.id, r.from, r.to, r.subject, r.content, r.created_at, r.updated_at, r.deleted_at]
    , map_video = (r, tags) => [r.id, r.file, r.created_at, r.updated_at, r.deleted_at, r.hash, tags.map(t => t.substring(0, 30))]
    , map_comment = r => [r.id, r.user_id, r.video_id, r.content, r.created_at, r.updated_at, r.deleted_at]
    , map_tag = (norm, name) => [norm.substring(0, 30), name.substring(0, 30)];

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
    //copy videos, messages and tags
    .then(() => {
        log_insert_start(["messages", "videos", "tags"]);
        return Promise.all([
            copy_table(select_full("messages"), insert_msgs, map_msg)
                .then(res => log_insert_done(res, "messages")),
            mydb.query(select_videos_tags)
                .then(videos => {
                    let tags = videos.map(v => v.tags.split(cfg.tag_select_separator))
                        .reduce((current, next) => current.concat(next));
                    return normalize_tags(tags)
                        .then(normalized => Promise.all([
                            Promise.all(normalized.map((norm, i) => pgdb.none(insert_tags, map_tag(norm, tags[i]))))
                                .then(res => log_insert_done(res, "tags")),
                            (() => {
                                let tag_start = 0,
                                    tag_map = [];
                                videos.forEach(v => {
                                    tag_map[v.id] = tag_start;
                                    tag_start += v.tags.split(cfg.tag_select_separator).length;
                                });
                                return Promise.all(videos.map(v => pgdb.none(insert_videos,
                                        map_video(v, normalized.slice().splice(tag_map[v.id],
                                            v.tags.split(cfg.tag_select_separator).length))
                                    )))
                                    .then(res => log_insert_done(res, "videos"));
                            })()
                        ]));
                })
        ])
        //copy comments
        .then(() => {
            log_insert_start(["comments"]);
            return copy_table(select_full("comments"), insert_comments, map_comment)
                .then(res => log_insert_done(res, "comments"));
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
            }),
            uploads_mapping = (m, v) => [m.get(v.user_id), v.id, v.created_at],
            favorites_mapping = (m, v) => [m.get(v.user_id), v.video_id, v.created_at];
        return Promise.all([
            fill_playlists("SELECT id, user_id, created_at FROM videos", "Uploads", uploads_mapping),
            fill_playlists("SELECT user_id, video_id, created_at FROM favorites", "Favorites", favorites_mapping)
        ]);
    })
    //set auto increment because ids may be missing in between in origin table
    .then(() => Promise.all(["comments", "messages", "users", "videos"]
            .map(table => pgdb.any("SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1")
                .then(row => pgdb.none("ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + (parseInt(row[0].id) + 1))
                    .then(() => console.log("adjusted auto increment for " + table + " table"))
                )
            )
    ))
    //cluster all tables because items are added asynchronously and thus are not in order
    .then(() => Promise.all(["comments", "messages", "playlist_video", "playlists", "users", "videos"]
            .map(table => pgdb.none("CLUSTER " + table + " USING " + table + "_pkey")
                .then(() => console.log("clustered " + table + " table"))
            )
    ))
    .catch(console.log) // error handling
    .then(() => { // close connections
        console.log("closing db connections...");
        mydb.end();
        pgdb.$pool.end();
        console.timeEnd("done! elapsed time");
    });

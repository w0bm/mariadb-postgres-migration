import my from "promise-mysql";
import pg from "pg-promise";
import cfg from "./config.json";
import normalize_tags from "./normalize_tags";
import column_sets from "./column_sets";
import queries from "./queries";

//create db connections
const pgp = new pg()
    , pgh = pgp.helpers
    , pgdb = (pgp)(cfg.postgres)
    , mydb = my.createPool(cfg.mysql);

//progress logging functions
const log = {
    copy_start: type => console.log("copying " + type + "..."),
    copy_done: (type, count) => console.log((count ? count + " " : "") + type + " copied")
}


//copy users and start timer
console.time("done! elapsed time");
log.copy_start("users");
mydb.query(queries.my.users).then(rows => {
        rows.map(r => {
            r.password = cfg.password_placeholder;
            r.categories = JSON.parse(r.categories);
        });
        return pgdb.none(pgh.insert(rows, column_sets.users))
            .then(() => log.copy_done("users", rows.length));
    })
    //normalize tags, then copy videos and tags
    .then(() => {
        log.copy_start("videos");
        return mydb.query(queries.my.videos_with_tags, cfg.tag_select_separator)
            .then(rows => normalize_tags(
                    rows.map(r => r.tags.split(cfg.tag_select_separator))
                        .reduce((cur, nxt) => cur.concat(nxt)),
                    cfg.tag_normalize_buffer
                )
                .then(tag_map => {
                    rows.map(r => r.tags = r.tags
                        .split(cfg.tag_select_separator)
                        .map(t => tag_map.get(t))
                    );
                    return pgdb.none(pgh.insert(rows, column_sets.videos))
                        .then(() => {
                            log.copy_done("videos", rows.length);
                            log.copy_start("tags");
                            return pgdb.none(pgh.insert(
                                [...tag_map].map(pair => Object.assign(
                                    {},
                                    {
                                        "tag": pair[0].substring(0, 30),
                                        "normalized": pair[1].substring(0, 30)
                                    }
                                )),
                                column_sets.tags
                            ) + " ON CONFLICT DO NOTHING")
                            .then(() => log.copy_done("tags"));
                        });
                })
            );
    })
    //copy comments and messages
    .then(() => {
        log.copy_start("comments");
        log.copy_start("messages");
        return Promise.all([
            mydb.query(queries.my.comments).then(rows =>
                pgdb.none(pgh.insert(rows, column_sets.comments))
                    .then(() => log.copy_done("comments", rows.length))
            ),
            mydb.query(queries.my.messages).then(rows =>
                pgdb.none(pgh.insert(rows, column_sets.messages))
                    .then(() => log.copy_done("messages", rows.length))
            )
        ]);
    })
    //link user favorites and uploads
    .then(() => {
        log.copy_start("uploads into playlist_video");
        log.copy_start("favorites into playlist_video");
        return pgdb.any(queries.pg.playlists).then(playlists => {
            const fill_playlists = (select_query, playlist_title) => {
                const user_playlist = new Map(playlists
                    .filter(pl => pl.title === playlist_title)
                    .map(pl => [pl.user_id, pl.id]
                        .map(str => parseInt(str))
                    )
                );
                return mydb.query(select_query).then(videos => {
                    pgdb.none(pgh.insert(
                        videos.map(v =>
                            Object.assign({}, {
                                playlist_id: user_playlist.get(v.user_id),
                                video_id: v.id || v.video_id,
                                created_at: v.created_at
                            })
                        ),
                        column_sets.playlist_video
                    ))
                });
            };
            return Promise.all([
                fill_playlists(queries.my.favorites, "Favorites")
                    .then(() => log.copy_done("favorites")),
                fill_playlists(queries.my.uploads, "Uploads")
                    .then(() => log.copy_done("uploads"))
            ]);
        });
    })
    //set auto increment because ids may be missing in between in origin table
    .then(() => Promise.all(["comments", "messages", "users", "videos"]
            .map(table => pgdb.any(queries.pg.max_id(table))
                .then(row => pgdb.none(queries.pg.set_auto_increment(table, parseInt(row[0].id) + 1))
                    .then(() => console.log("adjusted auto increment for " + table + " table"))
                )
            )
    ))
    //cluster all tables to make sure order is correct
    .then(() => Promise.all(["comments", "messages", "playlist_video", "playlists", "users", "videos"]
            .map(table => pgdb.none(queries.pg.cluster_pkey(table))
                .then(() => console.log("clustered " + table + " table"))
            )
    ))
    //catch errors
    .catch(console.log)
    //close database connections and print elapsed time
    .then(() => {
        console.log("closing db connections...");
        mydb.end();
        pgdb.$pool.end();
        console.timeEnd("done! elapsed time");
    });

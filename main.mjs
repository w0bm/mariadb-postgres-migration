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
};

const copy_users = async () => {
    log.copy_start("users");
    let users = await mydb.query(queries.my.users);
    users.map(u => {
        u.password = cfg.password_placeholder;
        u.categories = JSON.parse(u.categories);
    });
    return pgdb.none(pgh.insert(users, column_sets.users))
        .then(() => log.copy_done("users", users.length));
};

const copy_videos_and_tags = async () => {
    log.copy_start("videos");
    let videos = await mydb.query(queries.my.videos_with_tags, cfg.tag_select_separator);
    console.log("normalizing tags...");
    const tag_map = await normalize_tags(
        videos.map(v =>v.tags.split(cfg.tag_select_separator))
            .reduce((cur, nxt) => cur.concat(nxt)),
        cfg.tag_normalize_buffer
    );
    console.log("normalized tags")
    videos.map(v =>
        v.tags = v.tags
            .split(cfg.tag_select_separator)
            .map(t => tag_map.get(t))
    );
    await pgdb.none(pgh.insert(videos, column_sets.videos));
    log.copy_done("videos", videos.length);
    log.copy_start("tags");
    return pgdb.none(pgh.insert(
        [...tag_map].map(pair => {
            return {
                "tag": pair[0].substring(0, 30),
                "normalized": pair[1].substring(0, 30)
            }
        }),
        column_sets.tags
    ) + " ON CONFLICT DO NOTHING")
        .then(() => log.copy_done("tags"));
};

const copy_stuff = async mode => {
    if(!["comments", "messages"].includes(mode))
        return false;
    log.copy_start(mode);
    const tmp = await mydb.query(queries.my[mode]);
    return pgdb.none(pgh.insert(tmp, column_sets[mode]))
        .then(() => log.copy_done(mode, tmp.length));
};

//links uploads and favorites to their respective users
const fill_playlists = async () => {
    log.copy_start("uploads into playlist_video");
    log.copy_start("favorites into playlist_video");
    const playlists = await pgdb.any(queries.pg.playlists);
    const fill_playlists = async (select_query, playlist_title) => {
        const user_playlist = new Map(playlists
            .filter(pl => pl.title === playlist_title)
            .map(pl => [pl.user_id, pl.id]
                .map(str => parseInt(str))
            )
        );
        const videos = await mydb.query(select_query);
        return pgdb.none(pgh.insert(
            videos.map(v => {
                return {
                    playlist_id: user_playlist.get(v.user_id),
                    video_id: v.id || v.video_id,
                    created_at: v.created_at
                }
            }),
            column_sets.playlist_video
        ))
    };
    return Promise.all([
        fill_playlists(queries.my.favorites, "Favorites")
            .then(() => log.copy_done("favorites")),
        fill_playlists(queries.my.uploads, "Uploads")
            .then(() => log.copy_done("uploads"))
    ]);
};

const set_auto_increment = async tables => Promise.all(
    tables.map(table => pgdb.any(queries.pg.max_id(table))
        .then(row => pgdb.none(queries.pg.set_auto_increment(table, parseInt(row[0].id) + 1))
            .then(() => console.log("adjusted auto increment for " + table + " table"))
        )
    )
);

const cluster_tables = async tables => Promise.all(
    tables.map(table => pgdb.none(queries.pg.cluster_pkey(table))
        .then(() => console.log("clustered " + table + " table"))
    )
);

(async () => {
    try {
        console.time("done! elapsed time");
        await copy_users();
        await copy_videos_and_tags();
        await Promise.all([
            copy_stuff("comments"),
            copy_stuff("messages"),
            fill_playlists()
        ]);
        await set_auto_increment([
            "comments",
            "messages",
            "users",
            "videos"
        ]);
        await cluster_tables([
            "comments",
            "messages",
            "playlist_video",
            "playlists",
            "users",
            "videos"
        ]);
        console.timeEnd("done! elapsed time");
    }
    catch(error) {
        console.log(error);
    }
    finally {
        console.log("closing db connections...");
        mydb.end();
        pgdb.$pool.end();
    }
})();

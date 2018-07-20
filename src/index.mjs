import my from "promise-mysql";
import pg from "pg-promise";
import cfg from "../config.json";
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
        videos.map(v => v.tags.split(cfg.tag_select_separator))
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
        [...tag_map].map(pair => ({
            "tag": pair[0].substring(0, 30),
            "normalized": pair[1].substring(0, 30)
        })),
        column_sets.tags
    ) + " ON CONFLICT DO NOTHING")
        .then(() => log.copy_done("tags"));
};

const copy_comments = async () => {
    log.copy_start("comments");
    const comments = await mydb.query(queries.my.comments),
          response_regex = new RegExp(/^\^+/);
    let match, deleted_at;
    comments.forEach((c, index) => {
        if((match = response_regex.exec(c.content)) && (match = match[0])) {
            let deleted_offset = 0;
            for(let i = 1; i <= match.length + deleted_offset; i++) {
                if(index - i < 0 || comments[index - i].video_id !== c.video_id)
                    break;
                deleted_at = comments[index - i].deleted_at;
                if(deleted_at
                    && new Date(c.created_at).valueOf() > new Date(deleted_at).valueOf())
                    deleted_offset++;
                else if(i === match.length + deleted_offset)
                    comments[index].response_to = comments[index - i].id;
            }
        }
    });
    return pgdb.none(pgh.insert(comments, column_sets.comments))
        .then(() => log.copy_done("comments", comments.length));
};

const copy_messages = async () => {
    log.copy_start("messages");
    const messages = await mydb.query(queries.my.messages);
    return pgdb.none(pgh.insert(messages, column_sets.messages))
        .then(() => log.copy_done("messages", messages.length));
};

//links uploads and favorites to their respective users
const fill_playlists = async () => {
    log.copy_start("uploads into playlist_video");
    log.copy_start("favorites into playlist_video");
    const playlists = await pgdb.any(queries.pg.playlists);
    const fill_userplaylists_by_title = async (select_query, playlist_title) => {
        const user_playlist = new Map(playlists
            .filter(pl => pl.title === playlist_title)
            .map(pl => [pl.user_id, pl.id]
                .map(str => parseInt(str))
            )
        );
        const videos = await mydb.query(select_query);
        return pgdb.none(pgh.insert(
            videos.map(v => ({
                playlist_id: user_playlist.get(v.user_id),
                video_id: v.id || v.video_id,
                created_at: v.created_at
            })),
            column_sets.playlist_video
        ))
    };
    return Promise.all([
        fill_userplaylists_by_title(queries.my.favorites, "Favorites")
            .then(() => log.copy_done("favorites")),
        fill_userplaylists_by_title(queries.my.uploads, "Uploads")
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
            copy_comments(),
            copy_messages(),
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
        pgp.end();
    }
})();

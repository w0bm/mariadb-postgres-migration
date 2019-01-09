export default {
    my: {
        comments: "SELECT * FROM comments ORDER BY video_id, id ASC",
        messages: "SELECT * FROM messages",
        users: "SELECT * FROM users",
        favorites: "SELECT user_id, video_id, created_at FROM favorites",
        uploads: "SELECT id, user_id, created_at FROM videos",
        videos_with_tags: `SELECT v.*, GROUP_CONCAT(DISTINCT t.name SEPARATOR ?) as tags
                           FROM videos v, taggable_taggables tt, taggable_tags t
                           WHERE v.id = tt.taggable_id AND tt.tag_id = t.tag_id
                           GROUP BY v.id`
    },
    pg: {
        playlists: "SELECT id, user_id, title FROM playlists",
        max_id: table => "SELECT id FROM " + table + " ORDER BY id DESC LIMIT 1",
        set_auto_increment: (table, value) => "ALTER SEQUENCE " + table + "_id_seq RESTART WITH " + value,
        cluster_pkey: table => "CLUSTER " + table + " USING " + table + "_pkey"
    }
};

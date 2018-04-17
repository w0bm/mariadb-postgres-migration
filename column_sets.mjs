import pg from "pg-promise";

const pgh = new pg().helpers;

export default {
    users: new pgh.ColumnSet([
        {
            name: "id"
        },
        {
            name: "username"
        },
        {
            name: "password"
        },
        {
            name: "created_at"
        },
        {
            name: "updated_at"
        },
        {
            name: "deleted_at"
        },
        {
            name: "banned",
            prop: "banend"
        },
        {
            name: "banreason"
        },
        {
            name: "filters",
            prop: "categories"
        }
    ], { table: "users" }),

    videos: new pgh.ColumnSet([
        {
            name: "id"
        },
        {
            name: "file"
        },
        {
            name: "created_at"
        },
        {
            name: "updated_at"
        },
        {
            name: "deleted_at"
        },
        {
            name: "hash"
        },
        {
            name: "tags"
        }
    ], { table: "videos" }),

    tags: new pgh.ColumnSet([
        {
            name: "normalized"
        },
        {
            name: "tag"
        }
    ], { table: "tags" }),

    comments: new pgh.ColumnSet([
        {
            name: "id"
        },
        {
            name: "user_id"
        },
        {
            name: "video_id"
        },
        {
            name: "content"
        },
        {
            name: "created_at"
        },
        {
            name: "updated_at"
        },
        {
            name: "deleted_at"
        }
    ], { table: "comments" }),

    messages: new pgh.ColumnSet([
        {
            name: "id"
        },
        {
            name: "from_user",
            prop: "from"
        },
        {
            name: "to_user",
            prop: "to"
        },
        {
            name: "title",
            prop: "subject"
        },
        {
            name: "content"
        },
        {
            name: "created_at"
        },
        {
            name: "updated_at"
        },
        {
            name: "deleted_at"
        }
    ], { table: "messages" }),

    playlist_video: new pgh.ColumnSet([
        {
            name: "playlist_id"
        },
        {
            name: "video_id"
        },
        {
            name: "created_at"
        }
    ], { table: "playlist_video" })
};

import pg from "pg-promise";

const pgh = new pg().helpers;

export default {
    users: new pgh.ColumnSet([
        "id",
        "username",
        "password",
        "created_at",
        "updated_at",
        "deleted_at",
        {
            name: "banned",
            prop: "banend"
        },
        "banreason",
        {
            name: "filters",
            prop: "categories"
        }
    ], { table: "users" }),

    videos: new pgh.ColumnSet([
        "id",
        "file",
        "created_at",
        "updated_at",
        "deleted_at",
        "hash",
        "tags"
    ], { table: "videos" }),

    tags: new pgh.ColumnSet([
        "normalized",
        "tag"
    ], { table: "tags" }),

    comments: new pgh.ColumnSet([
        "id",
        "user_id",
        "video_id",
        "content",
        "created_at",
        "updated_at",
        "deleted_at"
    ], { table: "comments" }),

    messages: new pgh.ColumnSet([
        "id",
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
        "content",
        "created_at",
        "updated_at",
        "deleted_at"
    ], { table: "messages" }),

    playlist_video: new pgh.ColumnSet([
        "playlist_id",
        "video_id",
        "created_at"
    ], { table: "playlist_video" })
};

import pg from "pg-promise";

const pgh = new pg().helpers;

const DEFAULT = {
    rawType: true,
    toPostgres: () => "DEFAULT"
};

const ts_default_fallback = col => {
    if(col.value === null)
        return null;
    const date = new Date(col.value);
    if(isNaN(date.valueOf()))
        return DEFAULT;
    return col.value;
};

const catch_invalid = col =>
    typeof col === "string" ? {
        name: col,
        init: ts_default_fallback
    } : Object.assign(col, {
        init: ts_default_fallback
    });

export default {
    users: new pgh.ColumnSet([
        "id",
        "username",
        "password",
        "created_at",
        "updated_at",
        "deleted_at",
        catch_invalid({
            name: "banned",
            prop: "banend"
        }),
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
        "deleted_at",
        {
            name: "response_to",
            init: col => col.value || DEFAULT
        }
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
        catch_invalid("created_at")
    ], { table: "playlist_video" })
};

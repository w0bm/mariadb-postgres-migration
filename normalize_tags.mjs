import cp from "child_process";

export default (tags, bufferSize) => new Promise((resolve, reject) => {
    const child = cp.exec("normalize/target/release/normalize", { maxBuffer: bufferSize }, (err, stdout, stderr) => {
        if(!err && !stderr.length) {
            let normalized_tags = stdout.split("\n")
              , map = new Map();
            normalized_tags.pop();
            tags.forEach((t, i) => map.set(t, normalized_tags[i]));
            resolve(map);
        }
        reject({
            err: err,
            stdout: stdout.substr(0, 100) + "...",
            stderr: stderr
        });
    });
    child.stdin.write(tags.join("\n"));
    child.stdin.end();
});

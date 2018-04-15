import cp from "child_process";

export default tags => new Promise((resolve, reject) => {
    let child = cp.exec("normalize/target/release/normalize", null, (err, stdout, stderr) => {
        if(!err && !stderr.length) {
            let normalized_tags = stdout.split("\n");
            normalized_tags.pop();
            resolve(normalized_tags);
        }
        reject({
            err: err,
            stdout: stdout,
            stderr: stderr
        });
    });
    child.stdin.write(tags.join("\n"));
    child.stdin.end();
});

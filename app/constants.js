import fs from "fs";

let logDir = "/tmp/kraft-combined-logs";
if (process.argv.length > 2) {
    const propsPath = process.argv[2];
    try {
        const props = fs.readFileSync(propsPath, 'utf8');
        const match = props.match(/^log\.dirs=(.*)$/m);
        if (match) {
            logDir = match[1].trim();
        }
    } catch (e) {
        // ignore
    }
}

export const LOG_DIR = logDir;
export const LOG_PATH = `${logDir}/__cluster_metadata-0/00000000000000000000.log`;
export const ACTIVE_SEGMENT = "00000000000000000000.log";

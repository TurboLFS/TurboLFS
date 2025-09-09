const { spawn } = require('child_process');
const fs = require('fs');
const aggregator = require('./aggregator');
const agent = require('./agent');

main();

function main() {
    const argv = process.argv;
    const { options, positional } = parseArgs(argv);

    if (Object.keys(options).length === 0 && positional.length === 0) {
        printHelp();
        return;
    }

    const isTurbo = positional[0] === 'turbo';

    if (isTurbo) {
        turboMain(options, positional.slice(1));
    } else {
        gitMain(argv);
    }
}

function turboMain(options, positional) {

    const mode = positional[0];
    if (!mode) {
        console.error('Error: server or client is required.');
        return process.exit(1);
    }

    if (mode === "client") {
        const url = getFromEnvOrArgs(options, 'url');

        if (!url) {
            console.error('Error: --url is required for client mode.');
            return process.exit(1);
        }

        return agent.main(url);
    }

    if (mode === "server") {
        const port = getFromEnvOrArgs(options, 'port');
        const objectsDir = getFromEnvOrArgs(options, 'objects-dir');
        const objectsRepo = getFromEnvOrArgs(options, 'objects-repo');
        const objectRepoToken = getFromEnvOrArgs(options, 'object-repo-token');
        const s3Endpoint = getFromEnvOrArgs(options, 's3-endpoint');
        const s3AccessKey = getFromEnvOrArgs(options, 's3-access-key');
        const s3SecretKey = getFromEnvOrArgs(options, 's3-secret-key');
        const s3Bucket = getFromEnvOrArgs(options, 's3-bucket');
        const s3Region = getFromEnvOrArgs(options, 's3-region');
        if (!port) {
            console.error('Error: --port is required for server mode.');
            return process.exit(1);
        }
        if (objectsRepo && !objectRepoToken) {
            console.error('Error: --object-repo-token is required when --objects-repo is specified.');
            console.error('Tip: use --object-repo-token=PUBLIC for public repositories.');
            return process.exit(1);
        }

        const s3Options = [s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, s3Region];
        const numSet = s3Options.filter(x => x !== undefined).length;
        if (numSet > 0 && numSet < s3Options.length) {
            console.error('Error: When using S3 storage, all of --s3-endpoint, --s3-access-key, --s3-secret-key, --s3-bucket, and --s3-region must be specified.');
            return process.exit(1);
        }

        const success = aggregator.main(
            port,
            objectsDir,
            objectsRepo,
            objectRepoToken,
            s3Endpoint,
            s3AccessKey,
            s3SecretKey,
            s3Bucket,
            s3Region
        );

        if (!success) {
            return process.exit(1);
        }
    }
}

async function gitMain(argv) {
    let gitArgs = argv.slice(2); // Everything after `node src/index.js`

    // Parse --turbolfs-url and remove it from gitArgs
    let urlOption = "ws://localhost:3000";
    for (let i = 0; i < gitArgs.length; i++) {
        if (gitArgs[i] === '--turbolfs-url' && gitArgs[i + 1]) {
            urlOption = gitArgs[i + 1];
            gitArgs.splice(i, 2);
            break;
        } else if (gitArgs[i].startsWith('--turbolfs-url=')) {
            urlOption = gitArgs[i].slice('--turbolfs-url='.length);
            gitArgs.splice(i, 1);
            break;
        }
    }

    // TODO: support node/bun instead of compiled single binary
    // const exe = os.platform() === 'win32' ? 'turbolfs.exe' : 'turbolfs';
    //const exe = "/Users/leonidpospelov/projects/lfs-experiment/turbolfs"; // todo
    // let exe = "node";
    let exe = '';
    let argsPrefix = '';

    const isSingleExecutable = process.argv[0] === process.argv[1];
    if (isSingleExecutable) {
        exe = process.argv[1];
    } else {
        exe = process.argv[0];
        argsPrefix = process.argv[1] + ' ';

        // to unix path, even on Windows
        argsPrefix = argsPrefix.replace(/\\/g, '/');
    }

    // urlOption is now set above

    const gitPath = process.env.TURBOLFS_GIT_PATH || 'git';

    // GitHub Actions does "lfs install --local", intercepting that

    const isInGitRepo = fs.existsSync('.git');

    if (gitArgs[0] === 'lfs' && isInGitRepo) {
        const commandsToRun = [
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.path', exe],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.args', `${argsPrefix}turbo client "--url=${urlOption}"`],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.concurrent', 'true'],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.concurrenttransfers', '8'],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.direction', 'download'],
            ['config', 'set', '--local', 'lfs.standalonetransferagent', 'mybatcher'],
        ];

        for (const command of commandsToRun) {
            const gitConfigArgs = [...command];

            const gitConfig = spawn(gitPath, gitConfigArgs, {
                stdio: 'inherit'
            });

            await new Promise((resolve, reject) => {
                gitConfig.on('exit', code => {
                    if (code !== 0) {
                        console.error('Failed to run git config:', code);
                        reject(new Error(`git config exited with code ${code}`));
                    } else {
                        resolve();
                    }
                });

                gitConfig.on('error', err => {
                    console.error('Failed to run git config:', err.message);
                    reject(err);
                });
            });
        }
    }

    if (gitArgs[0] === 'clone') {
        const cloneUrlIndex = gitArgs.findIndex(arg => /^https?:\/\//.test(arg));
        if (cloneUrlIndex === -1) {
            console.error('No repository URL found in git clone command.');
            process.exit(1);
        }

        const configArgs = [
            '--config', `lfs.customtransfer.mybatcher.path=${exe}`,
            '--config', `lfs.customtransfer.mybatcher.args=${argsPrefix}turbo client "--url=${urlOption}"`,
            '--config', 'lfs.customtransfer.mybatcher.concurrent=true',
            '--config', 'lfs.customtransfer.mybatcher.concurrenttransfers=8',
            '--config', 'lfs.customtransfer.mybatcher.direction=download',
            '--config', 'lfs.standalonetransferagent=mybatcher'
        ];

        // Inject configArgs before the repository URL
        gitArgs = [
            ...gitArgs.slice(0, cloneUrlIndex),
            ...configArgs,
            ...gitArgs.slice(cloneUrlIndex)
        ];

        console.log('Running: git', gitArgs.join(' '));
    }

    const git = spawn(gitPath, gitArgs, {
        stdio: 'inherit'
    });

    git.on('exit', code => {
        process.exit(code);
    });

    git.on('error', err => {
        console.error('Failed to run git:', err.message);
        process.exit(1);
    });
}

function printHelp() {
    let commandBase = process.argv[0] + ' ' + process.argv[1];
    let isSea = process.argv[0] === process.argv[1];
    if (isSea) {
        commandBase = 'turbolfs';
    }

    console.log('\nUsage:');
    console.log('');
    console.log(`  # Server mode`);
    console.log(`  ${commandBase} turbo server --port=3000 --objects-dir=.git/lfs/objects --objects-repo=ORG/REPO --object-repo-token=TOKEN`);
    console.log('');
    console.log(`  # Client mode`);
    console.log(`  ${commandBase} turbo client --url=ws://localhost:3000`);
    console.log('');
    console.log(`  # Git command mode`);
    console.log(`  ${commandBase} clone <repo-url>`);
    console.log('');
    console.log('Options:');
    console.log('  --port                Port to run the server on (default: 3000)');
    console.log('  --objects-dir         Path to store or read objects');
    console.log('  --objects-repo        GitHub repo used for reading objects (format: owner/repo)');
    console.log('  --object-repo-token   GitHub token for uploading objects');
    console.log('  --s3-endpoint         S3-compatible storage endpoint URL');
    console.log('  --s3-access-key       S3 access key ID');
    console.log('  --s3-secret-key       S3 secret access key');
    console.log('  --s3-bucket           S3 bucket name');
    console.log('  --s3-region           S3 region');
    console.log('  --url                 Server URL for the client to connect to');
    console.log('');
    console.log('Examples:');
    console.log(`  ${commandBase} turbo server --port=3000 --objects-dir=.git/lfs/objects \\`);
    console.log(`      --objects-repo=fooorg/barrepo --object-repo-token=abc123`);
    console.log('');
    console.log(`  ${commandBase} turbo client --url=ws://localhost:3000`);
    console.log('');
    console.log(`  ${commandBase} clone https://github.com/org/repo.git`);
    console.log('');
}

function parseArgs(argv) {
    const args = argv.slice(2);
    const options = {};
    const positional = [];

    for (let i = 0; i < args.length; i++) {
        let arg = args[i];

        if (arg.startsWith('--')) {
            const [key, val] = arg.slice(2).split('=');
            if (val !== undefined) {
                options[key] = val;
            } else if (args[i + 1] && !args[i + 1].startsWith('-')) {
                options[key] = args[++i];
            } else {
                options[key] = true;
            }
        } else if (arg.startsWith('-') && arg.length > 1) {
            const key = arg.slice(1);
            if (args[i + 1] && !args[i + 1].startsWith('-')) {
                options[key] = args[++i];
            } else {
                options[key] = true;
            }
        } else {
            positional.push(arg);
        }
    }

    return { options, positional };
}

function getFromEnvOrArgs(options, key) {
    const envKey = key.toUpperCase().replace(/-/g, '_');

    const value1 = options[key];
    const value2 = process.env[envKey];
    if (value1 && value2) {
        console.error(`Error: Both --${key} and environment variable ${envKey} are set. Please set only one.`);
        process.exit(1);
    }
    return value1 || value2;
}

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
        const url = options.url;

        if (!url) {
            console.error('Error: --url is required for client mode.');
            return process.exit(1);
        }

        return agent.main(url);
    }

    if (mode === "server") {
        const port = options.port;
        const objectsDir = options['objects-dir'];
        const objectsRepo = options['objects-repo'];
        const objectRepoToken = options['object-repo-token'];
        if (!port) {
            console.error('Error: --port is required for server mode.');
            return process.exit(1);
        }
        if (objectsRepo && !objectRepoToken) {
            console.error('Error: --object-repo-token is required when --objects-repo is specified.');
            return process.exit(1);
        }

        const success = aggregator.main(port, objectsDir, objectsRepo, objectRepoToken);

        if (!success) {
            return process.exit(1);
        }
    }
}

async function gitMain(argv) {
    let gitArgs = argv.slice(2); // Everything after `node src/index.js`

    // TODO: support node/bun instead of compiled single binary
    // const exe = os.platform() === 'win32' ? 'turbolfs.exe' : 'turbolfs';
    //const exe = "/Users/leonidpospelov/projects/lfs-experiment/turbolfs"; // todo
    // let exe = "node";
    let exe = process.argv[0];

    const urlOption = "ws://localhost:3000"; // TODO

    const gitPath = process.env.TURBOLFS_GIT_PATH || 'git';

    console.error("!!!!!! gitArgs", gitArgs);

    // GitHub Actions does "lfs install --local", intercepting that

    const isInGitRepo = fs.existsSync('.git');

    if (gitArgs[0] === 'lfs' && isInGitRepo) {
        console.error('!!!!!!!!!!!!! Detected git lfs command, configuring custom transfer agent...');

        const commandsToRun = [
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.path', exe],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.args', `turbo client "--url=${urlOption}"`],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.concurrent', 'true'],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.concurrenttransfers', '8'],
            ['config', 'set', '--local', 'lfs.customtransfer.mybatcher.direction', 'download'],
            ['config', 'set', '--local', 'lfs.standalonetransferagent', 'mybatcher'],
        ];

        for (const command of commandsToRun) {
            const gitConfigArgs = [...command];

            console.error('!!!!!!! Running git config:', gitConfigArgs.join(' '));

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
            '--config', `lfs.customtransfer.mybatcher.args=turbo client "--url=${urlOption}"`,
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
    console.log('  --objects-repo        GitHub repo used for storing objects (format: owner/repo)');
    console.log('  --object-repo-token   GitHub token for uploading objects');
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

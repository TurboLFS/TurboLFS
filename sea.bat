node --experimental-sea-config sea-config.json

copy "C:\Program Files\nodejs\node.exe" .\turbolfs.exe

@REM npx -y esbuild --bundle --minify --target=node16 --platform=node --outfile=turbolfs_dist.js ./src/index.js

npx -y postject .\turbolfs.exe NODE_SEA_BLOB sea-prep.blob ^
  --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2

signtool remove /s .\turbolfs.exe

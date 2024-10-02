# Sync Servers
This is a lightweight server sync app designed for home servers. It instantly syncs changed files from one server to other servers, or perodically cron syncs.

A use case is to sync files to multiple servers without setting up cloud costly. This project is a component of my homeservers project, which leverage this to sync up my cam uploaded files to other servers.

Inotify is leveraged to monitor files changes, asynclcron is to run cron tasks, and rsync is to sync files.

The inotify recursive watch class has been moved/contributed to [asyncinotify](https://github.com/ProCern/asyncinotify).

## setup dev env
```bash
# in vscode terminal:
python3 -m venv venv
```

```bash
## reopen vscode terminal, venv should show
pip install asyncinotify
pip install asynclcron
```

## unit test
```bash
## run test
python3 -m unittest
```

## rsyncd
```
sudo cp tests/rsyncd/rsyncd.conf /etc/
sudo rsync --daemon --no-detach
```

## test rsync command
```bash
rsync -avh --no-p --mkpath --timeout=6 --contimeout=3 /storage/source/live rsync://127.0.0.1/live
```

## packaging and publish
```
rm -rf dist
python3 -m build
python3 -m twine upload dist/*
```

## run syncservers
```bash
# in root folder
python -m syncservers.app
```

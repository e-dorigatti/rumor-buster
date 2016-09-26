PowerShell.exe -Command "& workon webir ;  celery flower -A crawler.tasks --address=localhost --port=5555"
PAUSE
PowerShell.exe -Command "& docker run -d -v "$pwd\redisdata:/data" -p 6379:6379 redis"
PAUSE
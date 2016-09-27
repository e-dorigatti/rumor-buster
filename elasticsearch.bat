PowerShell.exe -Command "& echo $pwd ; docker run -d -p 9200:9200 -v "$pwd\esdata:/usr/share/elasticsearch/data" elasticsearch"
PAUSE
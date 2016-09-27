PowerShell.exe -Command "& docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/j4neo -v "$pwd\neo4jdata:/data" neo4j"
PAUSE
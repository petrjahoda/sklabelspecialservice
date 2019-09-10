﻿docker rmi -f petrjahoda/sklabelimportusers:latest
docker build -t petrjahoda/sklabelimportusers:latest .
docker push petrjahoda/sklabelimportusers:latest
#!/bin/bash
./node_modules/couchdb-dump/bin/cdbdump -d uwazi_development | ./node_modules/couchdb-dump/bin/cdbmorph -f ./morphs/extract_templates > templates.json

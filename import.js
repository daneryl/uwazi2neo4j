var neo4j = require('neo4j-driver').v1;
var fs = require('fs');
// Create a driver instance, for the user neo4j with password neo4j.
var driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "1234"));

// Create a session to run Cypher statements in.
// Note: Always make sure to close sessions when you are done using them!
var session = driver.session();

// Run a Cypher statement, reading the result in a streaming manner as records arrive:
fs.readFile('./database.json', function read(err, data) {
  if (err) {
    throw err;
  }

  var database = JSON.parse(data).docs;

  docs = {};

  database.forEach(function(doc){
    if(!docs[doc.type]) {
      docs[doc.type] = [];
    }
    docs[doc.type].push(doc);
  });

  console.log(Object.keys(docs));

  query("MATCH (n) DETACH DELETE n")
  .then(function() {
    return query(
      "WITH {json} as data UNWIND data as connection create (:Connection {id: connection._id, sourceDocument: connection.sourceDocument, targetDocument: connection.targetDocument})",
      {json: docs.reference}
    );
  })
  .then(function() {
    return query(
      "WITH {json} as data UNWIND data as document create (:Document {id: document._id, title: document.title, template: document.template})",
      {json: docs.document}
    );
  })
  .then(function() {
    return query(
      "WITH {json} as data UNWIND data as template create (:Template {id: template._id, name: template.name})",
      {json: docs.template}
    );
  })
  .then(function() {
    return query("MATCH (d:Document), (t:Template) WHERE EXISTS (d.template) AND EXISTS (t.id) AND d.template=t.id CREATE (d)-[:BELONGS]->(t)");
  })
  .then(function() {
    return query("MATCH (d:Document), (c:Connection) WHERE EXISTS (d.id) AND EXISTS (c.sourceDocument) AND d.id=c.sourceDocument CREATE (c)-[:ORIGINATES]->(d)");
  })
  .then(function() {
    return query("MATCH (d:Document), (c:Connection) WHERE EXISTS (d.id) AND EXISTS (c.targetDocument) AND d.id=c.targetDocument CREATE (c)-[:GOES_TO]->(d)");
  })
  .then(function() {
    console.log('IMPORT SUCCESFULL !');
    process.exit();
  })
  .catch(function(error) {
    console.log(error);
    process.exit()
  });
});

function query(query, params) {
  params = params || {};
  return new Promise(function(resolve, reject) {
    session.run(query, params)
    .subscribe({
      onCompleted: function() {
        resolve();
      },
      onError: function(error) {
        reject(error);
        //console.log(error);
        //process.exit();
      }
    });
  })
}

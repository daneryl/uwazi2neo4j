var neo4j = require('neo4j-driver').v1;
var fs = require('fs');
// Create a driver instance, for the user neo4j with password neo4j.
var driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "1234"), {
  encrypted: false
});

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


  // console.log(docs);
  console.log(Object.keys(docs));

  query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")
  // neccesary to delete db? we could use MERGE instead of CREATE and not deleting anything  



// -------------- Creating structure of nodes -----------------------------
 
  // .then(function() {
  //     return Promise.all(docs.document.map(function(document){
  //       var templateType = docs.template.find(function(template){
  //         return  template._id === document.template
  //       });
  //       if(!templateType){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "CREATE (:Documento:`"+templateType.name+"` {id: '"+document._id+"', type: '"+templateType.name+"'})");
  //   }))//                                                             , title: `"+document.title+"`, text: "+document.fullText+"
  // })
   // AND d2.id='"+reference.sourceDocument+"' AND d1.id='"+reference.targetDocument+"' CREATE (d1)-[:"+label+"]->(d2)");
        // "WITH {json} AS data UNWIND data AS document CREATE (:Document {id: document._id})",                


  // .then(function() {
  //   return query(
  //     "WITH {json} AS data UNWIND data AS metadata MATCH (d:Document) WHERE d.id=metadata._id SET d.title=metadata.title",
  //     {json: docs.document}
  //   );    
  // })

// MATCH (peter { name: 'Peter' })
// SET peter += { hungry: TRUE , position: 'Entrepreneur' }

        // "MATCH (d:Document) SET d.templateName = '"+templateType+"' ");









  // .then(function() {
  //   return Promise.all(docs.thesauri.map(function(thesauri){
  //     return query(
  //       "WITH {json} as data UNWIND data as thesaurivalue create (:ThesauriValue {label: thesaurivalue.label})",
  //       {json: thesauri.values}
  //     );    
  //   }))
  // })

  .then(function() {
      return Promise.all(docs.entity.map(function(entity){
        var templateType = docs.template.find(function(template){
          return  template._id === entity.template
        });
        if(!templateType){
          return Promise.resolve()
        }
      return query(
          "CREATE (:Entidad:`"+templateType.name+"` {id: '"+entity._id+"', type: '"+templateType.name+"' })");
    }))//                                                                                   , title: '"+entity.title+"'
  })

// Reading documento.metadata.resumen
  // .then(function() {
  //   return Promise.all(docs.entity.map(function(entity){
  //       if(!entity.metadata.resumen){
  //         return Promise.resolve()
  //       }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.type='Mecanismo' AND e.id= '"+entity._id+"' SET e.resumen=metadata.resumen",
  //       {json: entity.metadata}
  //     );    
  //   }))
  // })

// Judge and comision sex
  // .then(function() {
  //   return Promise.all(docs.entity.map(function(entity){
  //       if(!entity.metadata.sexo){
  //         return Promise.resolve()
  //       }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND e.id= '"+entity._id+"' AND metadata.sexo='1' SET e.sexo='Mujer' ",
  //       {json: entity.metadata}
  //     );    
  //   }))
  // })
  // .then(function() {
  //   return Promise.all(docs.entity.map(function(entity){
  //       if(!entity.metadata.sexo){
  //         return Promise.resolve()
  //       }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND e.id= '"+entity._id+"' AND metadata.sexo='2' SET e.sexo='Hombre' ",
  //       {json: entity.metadata}
  //     );    
  //   }))
  // })








// -------------- Creating relationships -----------------------------

  //two documents are related
  // .then(function() {
  //     return query("MATCH (ref:Reference), (rel:Relationtype), (relationtypeName:Relationtype) WHERE EXISTS (ref.relationType) AND EXISTS (rel.id) AND EXISTS (rel.name) AND ref.relationType=rel.id AND relationtypeName=rel.name CREATE (ref)-[:relationtypeName]->(rel)");
  // })

// DOCUMENT AND TEMPLATE READING WITHOUT TEMPLATE NODES
// .then(function() {
//       return Promise.all(docs.document.map(function(document){
//         var templateType = docs.template.find(function(template){
//           return  template._id === document.template
//         });
//         if(!templateType){
//           return Promise.resolve()
//         }
//         templateType = templateType.name.replace(" ","_").toUpperCase();
//       // AND d2.id='"+reference.sourceDocument+"' AND d1.id='"+reference.targetDocument+"' CREATE (d1)-[:"+label+"]->(d2)");
//       return query(
//         "MATCH (d:Document) SET d.templateName = '"+templateType+"' ");
//       }))//       WHERE d.template='"+templateType._id+"'
//   })
// CREATE (d1) - [:#{r.name}] -> (d2)





  // .then(function() {
  //     return Promise.all(docs.reference.map(function(reference){
  //       var doc = docs.document.find(function(document){
  //         return  document._id === reference.sourceDocument
  //       });
  //       var ent = docs.entity.find(function(entity){
  //         return  entity._id === reference.targetDocument
  //       });
  //       if(!doc || !ent){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "MATCH (d:Documento), (e:Entidad) WHERE e.type='País' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:HAPPENED_AT]->(e)");
  //   }))
  // })

  // .then(function() {
  //     return Promise.all(docs.reference.map(function(reference){
  //       var doc = docs.document.find(function(document){
  //         return  document._id === reference.sourceDocument
  //       });
  //       var ent = docs.entity.find(function(entity){
  //         return  entity._id === reference.targetDocument
  //       });
  //       if(!doc || !ent){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "MATCH (d:Documento), (e:Entidad) WHERE e.type='Causa' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:BELONGS_TO]->(e)");
  //   }))
  // })

  // .then(function() {
  //     return Promise.all(docs.reference.map(function(reference){
  //       var doc = docs.document.find(function(document){
  //         return  document._id === reference.sourceDocument
  //       });
  //       var ent = docs.entity.find(function(entity){
  //         return  entity._id === reference.targetDocument
  //       });
  //       if(!doc || !ent){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "MATCH (d:Documento), (e:Entidad) WHERE e.type='Mecanismo' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:CARRIED_BY]->(e)");
  //   }))
  // })

  // .then(function() {
  //     return Promise.all(docs.reference.map(function(reference){
  //       var doc = docs.document.find(function(document){
  //         return  document._id === reference.sourceDocument
  //       });
  //       var ent = docs.entity.find(function(entity){
  //         return  entity._id === reference.targetDocument
  //       });
  //       if(!doc || !ent){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "MATCH (d:Documento), (e:Entidad) WHERE e.type='Asunto' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:ASUNTOOOO]->(e)");
  //   }))
  // })

  // .then(function() {
  //     return Promise.all(docs.reference.map(function(reference){
  //       var doc = docs.document.find(function(document){
  //         return  document._id === reference.sourceDocument
  //       });
  //       var ent = docs.entity.find(function(entity){
  //         return  entity._id === reference.targetDocument
  //       });
  //       if(!doc || !ent){
  //         return Promise.resolve()
  //       }
  //     return query(
  //         "MATCH (d:Documento), (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:WAS_SIGNED_BY]->(e)");
  //   }))
  // })

// Relation between mechanisms and countries (WORK_IN)
  // .then(function() {
  //   return Promise.all(docs.entity.map(function(entity){
  //       if(!entity.metadata.paises){
  //         return Promise.resolve()
  //       }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS paisId MATCH (mecanismo:Entidad), (pais:Entidad) WHERE mecanismo.type='Mecanismo' AND pais.type='País' AND mecanismo.id='"+entity._id+"' AND paisId=pais.id CREATE (mecanismo)-[:WORK_IN]->(pais) ",
  //       {json: entity.metadata.paises}
  //     );    
  //   }))
  // })

// Relation between judges or comisions and countries (IS_FROM)
  .then(function() {
    return Promise.all(docs.entity.map(function(entity){
        if(!entity.metadata.pa_s){
          return Promise.resolve()
        }
      return query(
        "WITH {json} AS data UNWIND data AS metadata MATCH (juez:Entidad), (pais:Entidad) WHERE juez.type='Juez y/o Comisionado' AND pais.type='País' AND juez.id='"+entity._id+"' AND metadata.pa_s=pais.id CREATE (juez)-[:IS_FROM]->(pais) ",
        {json: entity.metadata}
      );    
    }))
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
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




// -------------------------- Creating structure of nodes --------------------------------------------------------


// Create nodes DOCUMENTO with their different labels (Sentencia, Orden...). By now missing the fullText
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
  //   }))//                                                             , text: "+document.fullText+"
  // })
// // Adding names of the documents (inside function before, issues while reading titles with special characters)
//   .then(function() {
//     return query(
//       "WITH {json} AS data UNWIND data AS metadata MATCH (d:Documento) WHERE d.id=metadata._id SET d.name=metadata.title ",
//       {json: docs.document}
//     );    
//   })


// Create nodes ENTIDAD with their different labels (Pais, Mecanismo, Causa...)
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
    }))
  })
// Adding names of the entities avoiding issues with special characters inside names
  .then(function() {
    return query(
      "WITH {json} AS data UNWIND data AS names MATCH (d:Entidad) WHERE d.id=names._id SET d.name=names.title ",
      {json: docs.entity}
    );    
  })


// // Adding resume to entities (MECHANISM, PROVISIONAL MEASSURE and CAUSE)  
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.resumen){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.id= '"+entity._id+"' SET e.resumen=metadata.resumen",
//         {json: entity.metadata}
//       );    
//     }))
//   })


// // Adding JUDGE/COMISION sex
// // Women
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.sexo){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND e.id= '"+entity._id+"' AND metadata.sexo='1' SET e.sexo='Mujer' ",
//         {json: entity.metadata}
//       );    
//     }))
//   })
// // Men
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.sexo){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS metadata MATCH (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND e.id= '"+entity._id+"' AND metadata.sexo='2' SET e.sexo='Hombre' ",
//         {json: entity.metadata}
//       );    
//     }))
//   })


  // Thesauri nodes (dictionaries)
  // .then(function() {
  //   return Promise.all(docs.thesauri.map(function(thesauri){
  //       if(!thesauri){
  //         return Promise.resolve()
  //       }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS values CREATE (:Dictionary:`"+thesauri.name+"` {value: values.id, name: values.label, type: '"+thesauri.name+"'}) ",
  //       {json: thesauri.values}
  //     );    
  //   }))
  // })



// -------------------------- Creating relationships ------------------------------------------------------

// USING REFERENCES (Documento--Entidad):

  // Documento --> [Reference] --> Pais (HAPPENED_AT)
//   .then(function() {
//       return Promise.all(docs.reference.map(function(reference){
//         var doc = docs.document.find(function(document){
//           return  document._id === reference.sourceDocument
//         });
//         var ent = docs.entity.find(function(entity){
//           return  entity._id === reference.targetDocument
//         });
//         if(!doc || !ent){
//           return Promise.resolve()
//         }
//       return query(
//           "MATCH (d:Documento), (e:Entidad) WHERE e.type='País' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:HAPPENED_AT]->(e)");
//     }))
//   })

//   // Documento --> [Reference] --> Causa (BELONGS_TO)
//   .then(function() {
//       return Promise.all(docs.reference.map(function(reference){
//         var doc = docs.document.find(function(document){
//           return  document._id === reference.sourceDocument
//         });
//         var ent = docs.entity.find(function(entity){
//           return  entity._id === reference.targetDocument
//         });
//         if(!doc || !ent){
//           return Promise.resolve()
//         }
//       return query(
//           "MATCH (d:Documento), (e:Entidad) WHERE e.type='Causa' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:BELONGS_TO]->(e)");
//     }))
//   })

//   // Documento --> [Reference] --> Mecanismo (CARRIED_BY)
//   .then(function() {
//       return Promise.all(docs.reference.map(function(reference){
//         var doc = docs.document.find(function(document){
//           return  document._id === reference.sourceDocument
//         });
//         var ent = docs.entity.find(function(entity){
//           return  entity._id === reference.targetDocument
//         });
//         if(!doc || !ent){
//           return Promise.resolve()
//         }
//       return query(
//           "MATCH (d:Documento), (e:Entidad) WHERE e.type='Mecanismo' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:CARRIED_BY]->(e)");
//     }))
//   })

//   // Documento --> [Reference] --> Asunto (ASUNTOOOO)
//   .then(function() {
//       return Promise.all(docs.reference.map(function(reference){
//         var doc = docs.document.find(function(document){
//           return  document._id === reference.sourceDocument
//         });
//         var ent = docs.entity.find(function(entity){
//           return  entity._id === reference.targetDocument
//         });
//         if(!doc || !ent){
//           return Promise.resolve()
//         }
//       return query(
//           "MATCH (d:Documento), (e:Entidad) WHERE e.type='Asunto' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:ASUNTOOOO]->(e)");
//     }))
//   })

//   // Documento --> [Reference] --> Juez y/o Comisionado (WAS_SIGNED_BY)
//   .then(function() {
//       return Promise.all(docs.reference.map(function(reference){
//         var doc = docs.document.find(function(document){
//           return  document._id === reference.sourceDocument
//         });
//         var ent = docs.entity.find(function(entity){
//           return  entity._id === reference.targetDocument
//         });
//         if(!doc || !ent){
//           return Promise.resolve()
//         }
//       return query(
//           "MATCH (d:Documento), (e:Entidad) WHERE e.type='Juez y/o Comisionado' AND d.id= '"+doc._id+"' AND e.id= '"+ent._id+"' CREATE (d)-[:WAS_SIGNED_BY]->(e)");
//     }))
//   })


// // USING ENTITIES (Entidad--Entidad):

//   // Relation between mechanisms and countries (WORK_IN)
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.paises){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS paisId MATCH (mecanismo:Entidad), (pais:Entidad) WHERE mecanismo.type='Mecanismo' AND pais.type='País' AND mecanismo.id='"+entity._id+"' AND paisId=pais.id CREATE (mecanismo)-[:WORK_IN]->(pais) ",
//         {json: entity.metadata.paises}
//       );    
//     }))
//   })

//   // Relation between judges or comisions and countries (IS_FROM)
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.pa_s){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS metadata MATCH (juez:Entidad), (pais:Entidad) WHERE juez.type='Juez y/o Comisionado' AND pais.type='País' AND juez.id='"+entity._id+"' AND metadata.pa_s=pais.id CREATE (juez)-[:IS_FROM]->(pais) ",
//         {json: entity.metadata}
//       );    
//     }))
//   })

//   // Relation between cause/subject and countries (REFERS_TO)
//   .then(function() {
//     return Promise.all(docs.entity.map(function(entity){
//         if(!entity.metadata.pa_s){
//           return Promise.resolve()
//         }
//       return query(
//         "WITH {json} AS data UNWIND data AS metadata MATCH (causa:Entidad), (pais:Entidad) WHERE NOT causa.type='Juez y/o Comisionado' AND pais.type='País' AND causa.id='"+entity._id+"' AND metadata.pa_s=pais.id CREATE (causa)-[:REFERS_TO]->(pais) ",
//         {json: entity.metadata}
//       );    
//     }))
//   })

  // Relation between cause/provisional meassure and "descriptores" (DESCRIBES/LABELED/AFFECTS/EXPLAIN)
  // .then(function() {
  //   return Promise.all(docs.entity.map(function(entity){
  //     if(!entity.metadata.descriptores){
  //       return Promise.resolve()
  //     }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS descriptoresId MATCH (dictionary:Dictionary {type: 'Descriptores'}), (causunto:Entidad) WHERE causunto.id='"+entity._id+"' AND descriptoresId=dictionary.value CREATE (causunto)-[:DESCRIBES]->(dictionary) ",
  //       {json: entity.metadata.descriptores}
  //     );    
  //   }))
  // })

  // Relation between "documento" and thesauri "tipo" (IS_TYPE)
  // .then(function() {
  //   return Promise.all(docs.document.map(function(document){
  //     if(!document.metadata.tipo){
  //       return Promise.resolve()
  //     }
  //     return query(
  //       "WITH {json} AS data UNWIND data AS tipoId MATCH (dictionary:Dictionary {type: 'Tipos'}), (doc:Documento) WHERE doc.id='"+document._id+"' AND tipoId=dictionary.value CREATE (doc)-[:IS_TYPE]->(dictionary) ",
  //       {json: document.metadata.tipo}
  //     );    
  //   }))
  // })

  // Relation between cause and provisional meassure (if they exist) (cause CONTAINS a provisional meassure)
  .then(function() {
    return query(
      "MATCH (causa:Causa) WITH causa MATCH (asunto:Asunto) WHERE causa.name=asunto.name CREATE (causa)-[:CONTAINS]->(asunto) ");    
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
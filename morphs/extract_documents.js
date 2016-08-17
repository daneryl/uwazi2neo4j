module.exports = function(doc, cb){
  if(doc.type === 'document'){
    delete doc.fullText;
    return cb(null, doc); //unmodified doc is passed through to the output
  }
  return cb(); //doc will not be in the output
}

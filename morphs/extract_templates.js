module.exports = function(doc, cb){
  if(doc.type === 'template'){
    return cb(null, doc); //unmodified doc is passed through to the output
  }
  return cb(); //doc will not be in the output
}

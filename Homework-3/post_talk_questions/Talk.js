const mongoose = require('mongoose');

/*const choice=new mongoose.Schema({
    choice: String
},{ _id : false })*/

const question=new mongoose.Schema({
    question: String,
    type: String,
    answer: String,
},{ _id : false, strict:false})

const talk_schema = new mongoose.Schema({
    _id: String,
    questions: [question]
}, { collection: 'triviated' });

module.exports = mongoose.model('talk', talk_schema);
const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.add_questions = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id) {
        callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Id is null.'
        })
    }
    if(!body.questions) {
        callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Questions is null.'
        })
    }
    
    body.questions.forEach(function(item, index){
        if(!item.question || !item.answer || !item.type){
            callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Questions need 3 fields: question, answer, type'
            })
        }
        if(item.type!="open" && item.type!="choice" && item.type!="v/f"){
            callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Type can only be open, choice or v/f.'
            })
        }
        if(item.type=="choice" && (!item.choices || item.choices.length!=4)){
            callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Choice questions need 4 choices.'
            })
        }
        if(item.type=="v/f" && item.answer!="False" && item.answer!="True"){
            callback(null, {
                    statusCode: 400,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'V/F questions can only be True or False.'
            })
        }
    })
    
    connect_to_db().then(() => {
        console.log('=> add questions');
        talk.findByIdAndUpdate(body.id ,{ "$push": {"questions": body.questions }}, {omitUndefined: true})
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        headers: { 'Content-Type': 'text/plain' },
                        body: 'Questions added.'
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};
const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.get_questions = (event, context, callback) => {
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
    
    connect_to_db().then(() => {
        console.log('=> add questions');
        talk.findOne({_id:body.id},'_id questions').where()
            .then(questions => {
                    callback(null, {
                        statusCode: 200,
                        headers: { 'Content-Type': 'text/plain' },
                        body: JSON.stringify(body.type && (body.type=="choice" || body.type=="v/f" || body.type=="open") ? {_id: questions._id, questions: questions.questions.filter(item=>item.type==body.type)} : questions)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the questions.'
                })
            );
    });
};
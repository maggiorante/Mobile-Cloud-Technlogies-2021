const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.get_watchnext_by_id = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id) {
        callback(null, {
                    statusCode: 404,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Id is null.'
        })
    }
    
    /*var projection= {}
    if (body.projection) {
        projection = { watch_next: { $projection: body.projection} }
    }*/
    
    connect_to_db().then(() => {
        console.log('=> get_all watchnext');
        talk.findOne({_id: body.id})
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks.watch_next)
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
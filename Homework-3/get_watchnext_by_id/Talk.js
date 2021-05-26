const mongoose = require('mongoose');

const watchnext_schema = new mongoose.Schema({
    link: String,
    watch_next_idx: String,
    title: String,
    main_speaker: String,
    posted: String,
});

const talk_schema = new mongoose.Schema({
    _id: String,
    watch_next: [watchnext_schema]
}, { collection: 'triviated' });

module.exports = mongoose.model('talk', talk_schema);
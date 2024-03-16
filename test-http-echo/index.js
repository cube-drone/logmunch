const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port_str = process.env.PORT || "9283";
const port = parseInt(port_str);

app.use(bodyParser.text({
    type: function(req) {
        return true;
    }
}));
//app.use(express.urlencoded({ extended: true }));

app.get('/*', (req, res) => {
    let log = `GET ${req.url} ${JSON.stringify(req.query)} ${JSON.stringify(req.headers)}`;
    console.log(log);
    res.send('Hello World!')
});

function splunkparse(body){
    let list = [];
    let buffer = "";
    for (let character of body.split("")){
        if (character == "}"){
            try{
                list.push(JSON.parse(buffer + "}"));
            }
            catch(e){
                list.push(buffer + "}");
            }
            buffer = "";
        } else {
            buffer += character;
        }
    }
    return list;
}

app.post('/*', (req, res) => {
    let log = `POST ${req.url} ${JSON.stringify(req.query)}`;
    console.log("-----------")
    console.log(log);
    console.log("HEADERS: ");
    console.dir(req.headers);
    console.log("BODY: ");
    console.dir(splunkparse(req.body));
    console.log(" ");
    res.send('Hello World!');
});

app.put('/*', (req, res) => {
    let log = `PUT ${req.url} ${JSON.stringify(req.query)} ${JSON.stringify(req.headers)}`;
    console.log(req.body);
    console.log(log);
    res.send('Hello World!')
});

app.delete('/*', (req, res) => {
    let log = `DELETE ${req.url} ${JSON.stringify(req.query)} ${JSON.stringify(req.headers)}`;
    console.log(log);
    res.send('Hello World!')
});

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})
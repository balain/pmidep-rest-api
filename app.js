/*******************************
 * app.js
 *
 * Created: 2/29/2020
 * Author: John D. Lewis
 * License: MIT
 *******************************/

let auth = require('./auth.js');
let config = require('./config.js');
let depfunc = require('./depfunc.js');

const request = require('request');
const port = config.port;

var options = {
  key: auth.key,
  certificate: auth.cert
};

var restify = require('restify');
var sqlite3 = require('sqlite3').verbose();

// Restify setup
var server = restify.createServer(options);
server.use(restify.plugins.queryParser());

server.use(
  function crossOrigin(req,res,next){
    if (config.okOrigins.includes(req.headers.origin)) {
      res.header("Access-Control-Allow-Origin", req.headers.origin);
      res.header("Access-Control-Allow-Headers", "X-Requested-With");
      console.log("Serving request to ", req.headers.origin);
    }
    return next();
  }
);

// Restify auth
server.use(function(req, res, next) {
	if (req.query.a) {
		if (req.query.a == auth.adminPass) {
			return next();
		} else {
			console.log("invalid 'a' value provided");
			res.send(404);
			return next();
		}
	} else if (req.query.admin) {
		let options = {json: true};
		request(config.urlBase + req.query.admin, options, (error, res2, body) => {
			if (error) { console.log(error); res.send(404); return next(); }
			else if (!error && res.statusCode == 200) {
				if (body && body['result'] == 1) {
					console.log("success", req.connection.remoteAddress);
					return next();
				} else {
					console.log("no body or result != 1", req.connection.remoteAddress);
					res.send(404);
					return next(false);
				}
			}
		});
	} else {
		console.log("missing admin", req.connection.remoteAddress);
		res.send(404);
		return next(false);
	}
})

// API methods
server.get('/', function(req, res, next) { res.send(404); return next(false); });
server.get('/lastname/:lastname', findLastName);
server.get('/id/:id', findId);
server.get('/report', generateReport);
server.get('/reportSimple', generateReportSimple);

// API functions
function findLastName(req, res, next) {
	depfunc.findLastName(req.params.lastname)
		.then(results => {
	    	res.send({errors: [], meta: results.meta, data: results.data});
		}).catch(err => {
			res.send({errors: [err], meta: [], data: []});
		})
	next();
}

function findId(req, res, next) {
	depfunc.findId(req.params.id).then((result) => {
			res.send({errors: [], meta: result.meta, data: result.data});
		}).catch((err) => {
	    	res.send({errors: [err], meta: [], data: []});
		}
	);
	next();
}

function generateReport(req, res, next) {
	depfunc.report().then((result) => {
		res.send({errors: [], meta: [], data: result});
	});
	next();
}
function generateReportSimple(req, res, next) {
	depfunc.reportSimple().then((results) => {
		res.send({errors: [], meta: [], data: results});
	});
	next();
}

process.on('SIGINT', function() {
	depfunc.closeDb()
		.then(process.exit());
})

server.listen(port, () => {
  console.log("server starting on port : " + port)
});

var http = require('http');
var fs = require('fs');
var path = require('path');
var csv = require('csv-parser');
var stringSimilarity = require('string-similarity');
var formidable = require('formidable');

var PORT = 3000;
let entities = [];

// Here we go and load the default data...
function loadDefaultData() {
	entities = [];
	fs.createReadStream(path.join(__dirname, 'canada_cities/canadacities.csv'))
		.pipe(csv())
		.on('data', function (row) {
			if (!row.city) return;
			entities.push({
				id: row.id || row.city,
				name: row.city,
				type: 'City',
				province: row.province || row.admin_name || '',
				latitude: parseFloat(row.lat || row.latitude) || null,
				longitude: parseFloat(row.lng || row.longitude) || null
			});
		})
		.on('end', function () {
			console.log('Default CSV data loaded successfully');
			console.log(`Loaded ${entities.length} entities`);
		})
		.on('error', function(err) {
			console.error('Error loading default data:', err);
		});
}

// We utilize this function to process CSV data ensuring that it correctly gets parsed
function processUploadedCSV(filePath, callback) {
	var newEntities = [];
	fs.createReadStream(filePath)
		.pipe(csv())
		.on('data', function (row) {
			var cityName = row.city || row.name || row.City || row.Name;
			if (!cityName) return;

			newEntities.push({
				id: row.id || cityName,
				name: cityName,
				type: 'City',
				province: row.province || row.admin_name || row.state || '',
				latitude: parseFloat(row.lat || row.latitude || row.Latitude) || null,
				longitude: parseFloat(row.lng || row.longitude || row.Longitude) || null
			});
		})
		.on('end', function () {
			entities = newEntities;
			console.log(`Uploaded CSV processed: ${entities.length} entities`);
			callback(null, entities.length);
		})
		.on('error', function(err) {
			console.error('Error processing uploaded CSV:', err);
			callback(err);
		});
}

function calculateScore(query, entity) {
	var queryLower = query.toLowerCase();
	var nameLower = entity.name.toLowerCase();

	// exact matches return a score of 100%
	if (queryLower === nameLower) {
		return 100;
	}

	var stringSim = stringSimilarity.compareTwoStrings(queryLower, nameLower);
	let prefixBonus = 0;
	if (nameLower.startsWith(queryLower)) {
		prefixBonus = 0.3;
	} else if (nameLower.includes(queryLower)) {
		prefixBonus = 0.15;
	}

	var typeBonus = 0.2;
	var finalScore = (stringSim * 0.6) + prefixBonus + typeBonus;
	return Math.round(finalScore * 100);
}

loadDefaultData();
var server = http.createServer(function(req, res) {
	var url = new URL(req.url, `http://${req.headers.host}`);
	var method = req.method;
	var contentType = req.headers['content-type'] || '';
	console.log(`${method}: ${url.pathname}`);
	var corsHeaders = {
		'Access-Control-Allow-Origin': '*',
		'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
		'Access-Control-Allow-Headers': 'Content-Type'
	};

	if (method === 'OPTIONS') {
		res.writeHead(200, corsHeaders);
		res.end();
		return;
	}

	if (url.pathname === '/reconcile' && method === 'GET') {
		var metadata = {
			name: 'Canadian Cities Reconciliation Service',
			identifierSpace: 'http://localhost:3000/entities/',
			schemaSpace: 'http://localhost:3000/schema/',
			defaultTypes: [
				{ id: 'city', name: 'City' }
			],
			view: {
				url: 'http://localhost:3000/entities/{{id}}'
			}
		};

		res.writeHead(200, {
			'Content-Type': 'application/json',
			...corsHeaders
		});
		res.end(JSON.stringify(metadata));
		return;
	}

	if (url.pathname === '/reconcile' && method === 'POST') {
		let body = '';
		req.on('data', function(chunk) {
			body += chunk.toString();
		});
		req.on('end', function() {
			let parsed;
			try {
				if (contentType.includes('application/x-www-form-urlencoded')) {
					var params = new URLSearchParams(body);
					var rawQueries = params.get('queries') || params.get('query');
					parsed = {
						queries: JSON.parse(rawQueries)
					};
				} else if (contentType.includes('application/json')) {
					parsed = JSON.parse(body);
				} else {
					throw new Error('Unsupported Content-Type');
				}
			} catch (err) {
				res.writeHead(400, {
					'Content-Type': 'application/json',
					...corsHeaders
				});
				return res.end(JSON.stringify({
					error: 'Invalid request format',
					message: err.message
				}));
			}

			var queries = parsed.queries || { q0: parsed.query };
			var results = {};

			for (let key in queries) {
				var userQuery = (queries[key].query || '').trim();
				if (!userQuery) {
					results[key] = { result: [] };
					continue;
				}

				// Calculate scores for all entities
				var scoredEntities = entities.map(function(entity) {
					var score = calculateScore(userQuery, entity);
					return {
						id: entity.id || entity.name || '',
						name: entity.name || '',
						score: score,
						match: score > 85, // High confidence threshold
						type: [
							{
								id: 'city',
								name: 'City'
							}
						]
					};
				});

				// Sort by score and limit results
				var matches = scoredEntities
					.filter(entity => entity.score > 10) // Minimum threshold
					.sort(function(a, b) {
						return b.score - a.score;
					})
					.slice(0, queries[key].limit || 5);

				results[key] = { result: matches };
			}

			res.writeHead(200, {
				'Content-Type': 'application/json',
				...corsHeaders
			});
			res.end(JSON.stringify(results));
		});
		return;
	}

	if (url.pathname === '/upload' && method === 'POST') {
		let body = '';
		let boundary = '';
		var contentType = req.headers['content-type'] || '';
		var boundaryMatch = contentType.match(/boundary=(.+)$/);
		if (boundaryMatch) {
			boundary = '--' + boundaryMatch[1];
		}

		req.on('data', function(chunk) {
			body += chunk.toString('binary');
		});

		req.on('end', function() {
			if (!boundary) {
				res.writeHead(400, {
					'Content-Type': 'application/json',
					...corsHeaders
				});
				return res.end(JSON.stringify({
					error: 'Invalid multipart request'
				}));
			}
			var parts = body.split(boundary);
			let csvContent = '';

			for (let part of parts) {
				if (part.includes('filename=') && part.includes('.csv')) {
					// Extract just the CSV content (after the headers)
					var contentStart = part.indexOf('\r\n\r\n') + 4;
					csvContent = part.substring(contentStart).replace(/\r\n$/, '');
					break;
				}
			}

			if (!csvContent) {
				res.writeHead(400, {
					'Content-Type': 'application/json',
					...corsHeaders
				});
				return res.end(JSON.stringify({
					error: 'No CSV content found'
				}));
			}
			var tempFile = path.join(__dirname, 'temp_upload.csv');
			fs.writeFileSync(tempFile, csvContent);
			processUploadedCSV(tempFile, function(err, recordCount) {
				fs.unlink(tempFile, () => {});
				if (err) {
					res.writeHead(500, {
						'Content-Type': 'application/json',
						...corsHeaders
					});
					return res.end(JSON.stringify({
						error: 'Processing failed',
						message: err.message
					}));
				}

				res.writeHead(200, {
					'Content-Type': 'application/json',
					...corsHeaders
				});
				res.end(JSON.stringify({
					success: true,
					message: 'Dataset uploaded successfully',
					recordCount: recordCount,
					fields: ['city', 'province', 'latitude', 'longitude']
				}));
			});
		});
		return;
	}
	if (url.pathname === '/status' && method === 'GET') {
		res.writeHead(200, {
			'Content-Type': 'application/json',
			...corsHeaders
		});
		res.end(JSON.stringify({
			status: 'running',
			entities: entities.length,
			service: 'Canadian Cities Reconciliation Service'
		}));
		return;
	}
	res.writeHead(404, {
		'Content-Type': 'application/json',
		...corsHeaders
	});
	res.end(JSON.stringify({
		error: 'Not found',
		message: 'Endpoint not found'
	}));
});

server.listen(PORT, function(){
	console.log(`Reconciliation API is running at http://localhost:${PORT}/reconcile`);
	console.log(`Upload endpoint available at http://localhost:${PORT}/upload`);
	console.log(`Status endpoint available at http://localhost:${PORT}/status`);
});

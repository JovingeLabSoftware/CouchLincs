var config = {
	'prod': {
 	    'version': 1.0,
  		'couchdb': {
			'ip': '52.35.203.212',
			'bucket': 'LINCS',
			'port': 8091,
			'password': null
		}
	},
	'devel': {
 	    'version': 1.0,
  		'couchdb': {
			'ip': '52.35.203.212',
			'bucket': 'LINCS',
			'port': 8091,
			'password': null
		}
	}
};

if(process.env.LINCS_DEVEL){
	module.exports = config.devel;
} else {
	module.exports = config.prod;	
}

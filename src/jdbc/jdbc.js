'use strict';

let jvm = require('jdbc/lib/jinst')
let _jdbc = require('jdbc')
let AWS = require('aws-sdk')

/* Load JVM options and class path definitions before JVM is actually created */
if (!jvm.isJvmCreated()) {
	jvm.addOption("-Xrs")
	jvm.setupClasspath([
		'./drivers/AthenaJDBC41-1.0.1.jar',  //Athena JDBC driver
		'./drivers/customsessioncredentialsprovider-1.1.jar',  //custom session credential provider to get ECS temp credentials to work
		'./drivers/aws-java-sdk-1.11.114.jar',  //AWS SDK
		'./drivers/aws-java-sdk-core-1.11.114.jar'])
}

/* JDBC driver wrapper
** Parameters Object:
*** region: AWS region where your Athena service is configured.  Default to us-east-1
*** minpool: Minimum number of JDBC connections the wrapper will maintain
*** maxpool: Maximum number of JDBC connections the wrapper will allow
*** aws_profile: When using credential file, this is the profile name to look fo
*** cred_filename: The path and filename where to find the AWS credentials file
*** s3_staging_dir: The S3 bucket that Athena uses for temporary files
*** log_path: Athena JDBC log4j path inside the local ECS container
*** log_level: log4j logging level
*** user: AWS AccessKeyId - not used unless credentials are hard coded which is not recommended
*** password: AWS SecretAccessKey - not used unless credentials are hard coded which is not recommended
*/

const CustomCredentialProviderClass = 'com.amazonaws.athena.jdbc.CustomSessionCredentialsProvider'  //class name of the custom credential provider.  Not required if using credential file
const AthenaJdbcDriverClassName = 'com.amazonaws.athena.jdbc.AthenaDriver'
const AthenaConnectionUrl = 'jdbc:awsathena://athena.%.amazonaws.com:443'
const DefaultAwsRegion = 'us-east-1'
const DefaultLogPath = '../logs/athenajdbc.log'
const DefaultLogLevel = 'TRACE'
const MinPoolSize = 1
const MaxPoolSize = 2

class JDBC {
	constructor(params) {
		return new Promise((resolve, reject) => {
			if (params) {
				this.region = ('region' in params) ? params.region : DefaultAwsRegion
				this.url = AthenaConnectionUrl.replace(/%/, this.region)
				this.driver = AthenaJdbcDriverClassName
				/* JDBC wrapper creates a pool of connections to the backend, these define the pool size */
				this.minpool = ('minpool' in params) ? params.minpool : MinPoolSize
				this.maxpool = ('maxpool' in params) ? params.maxpool : MaxPoolSize
				this.properties = {
					log_path : ('log_path' in params) ? params.log_path : DefaultLogPath,
      				log_level: ('log_level'in params) ? params.log_level : DefaultLogLevel,
					aws_credentials_provider_class: CustomCredentialProviderClass
				}
				if ('s3_staging_dir' in params) this.properties.s3_staging_dir = params.s3_staging_dir
				else return reject(new Error ('"s3_staging_dir" property is missing from parameters passed to JDBC constructor'))
				
				return this.initAwsCredentials(params).then(() => {
					let config = this.buildAthenaConfig()
	  				this.JdbcConnection = new _jdbc(config)
	  				return resolve(this)
				}).catch((err) => { return reject(err) })
	  		} else return reject(new Error('Missing configuration parameters'))
	  	})
	}

	initAwsCredentials (params) {
		return new Promise((resolve, reject) => {
			if ('accessKeyId' in params && 'secretAccessKey' in params){
				this.properties.user = params.accessKeyId
	    		this.properties.password = params.secretAccessKey
	    		return resolve()
			} else if ('aws_profile' in params) {
				let creds = new AWS.SharedIniFileCredentials({
					profile: params.aws_profile,
					filename: ('cred_filename' in params) ? params.cred_filename : undefined
				})
				if (creds.accessKeyId != undefined) {
					this.properties.user = creds.accessKeyId
					this.properties.password = creds.secretAccessKey
					return resolve()
				} else return reject(new Error (`Credentials for profile ${params.aws_profile} not found, try passing the full path in the "cred_filename" param to the JDBC constructor`))
			} else {
				AWS.config.credentials = new AWS.ECSCredentials({
					httpOptions: { timeout: 5000 }, // 5 second timeout
					maxRetries: 10, // retry 10 times
					retryDelayOptions: { base: 200 } // see AWS.Config for information
				})
				AWS.config.credentials.refresh((err) => {
					if (err) console.log('ECS Credentials Error Detail: ', err)
					if (AWS.config.credentials.accessKeyId != undefined) {
						this.properties.aws_credentials_provider_arguments = `${AWS.config.credentials.accessKeyId},${AWS.config.credentials.secretAccessKey},${AWS.config.credentials.sessionToken}`
						return resolve()
					} else return reject(new Error('Failed to get ECS credentials'))
				})
			}
		})
	}

	buildAthenaConfig () {
		return {
		  url: this.url,
		  minpoolsize: this.minpool,
		  maxpoolsize: this.maxpool,
		  drivername: this.driver,
		  properties: this.properties
		}
	}

	createConnection() {
		let self = this
		return new Promise((resolve, reject) => {
		    self.JdbcConnection.initialize((err) => {
				if (err) return reject(err)
				self.JdbcConnection.reserve((err, connObj) => {
			  		if (err || !connObj) return reject(err)
			  		self._conn = connObj.conn
			    	return resolve(self._conn)
			  	})
		    })
		})
	}

	closeConnection() {
		let self = this
		return new Promise((resolve, reject) => {
	    	self.JdbcConnection.release(self._conn, (err) => {
	      		if (err) return reject(err)
	      		return resolve()
	    	})
	  	})
	}

	query(sql) {
		let self = this
	 	return new Promise((resolve, reject) => {
	    	self._conn.createStatement((s_err, statement) => {
		      	if (s_err) return reject(s_err)
		      	statement.executeQuery(sql, (q_err, results) => {
		        	if (q_err) return reject(q_err)
		        	results.toObjArray((e, data) => { 
		          		if (e) return reject (e)
		          		return resolve(data)
		        	})
		      	})
	    	})
	  	})
	}

	isConnected() {
		return !!this._conn
	}

	setRegion (region) {
		//TODO
	}

	setMinPoolSize (num) {
		//TODO
	}

	setMaxPoolSize (num) {
		//TODO
	}
}


module.exports = JDBC

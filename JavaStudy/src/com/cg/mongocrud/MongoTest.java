
package com.cg.mongocrud;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBPort;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoTest
{

	private static SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd" );
	private static DB db = null;

	public static DB getDb( )
	{
		return db;
	}

	public static void main( String[] args ) throws Exception
	{
		init( );
		// fsfind();
		for ( int i = 0; i < 5000; i++ )
		{
			girdfstest( );
		}
		// insertDoc();
		// findDocs();
	}

	/*
	 * 三种验证方式连接mongo数据库
	 */
	private static void init( ) throws Exception
	{
		// initByURI();
		// initByCredential_1();
		initByCredential_2( );

	}

	private static void fsfind( )
	{
		if ( db == null )
		{
			return;
		}
		GridFS gridFS = new GridFS( db );
		List<GridFSDBFile> find = gridFS.find( "1456366441428.html" );
		System.out.println( find.toString( ) );
	}

	private static void initByURI( )
	{
		try
		{
			// MongoClientURI的验证方式
			String sURI = String.format( "mongodb://%s:%s@%s:%d/%s", "kyd",
					"kyd", "192.168.99.158", 40000, "kydtestdb" );
			MongoClientURI uri = new MongoClientURI( sURI );
			MongoClient mongoClient = new MongoClient( uri );
		}
		catch ( UnknownHostException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace( );
		}
	}

	private static void initByCredential_1( ) throws UnknownHostException
	{
		// MongoCredential的验证方式
		MongoCredential credential = MongoCredential.createMongoCRCredential(
				"kyd", "kydtestdb", "kyd".toCharArray( ) );
		ServerAddress serverAddress = new ServerAddress( "192.168.99.157",
				40000 );
		MongoClient mongoClient = new MongoClient( serverAddress,
				Arrays.asList( credential ) );

		db = mongoClient.getDB( "kydtestdb" );
	}

	private static void initByCredential_2( )
	{
		// 生产环境用的MongoCredential验证方式
		String dbHost = "192.168.99.158:40000";
		String dbName = "kydtestdb";
		String dbUserName = "kyd";
		String dbPassword = "kyd";
		List<ServerAddress> addresses = new ArrayList<ServerAddress>( );
		try
		{
			String[] serverAddrs = dbHost.split( "," );

			for ( String serverAddr : serverAddrs )
			{
				ServerAddress address = new ServerAddress( serverAddr,
						DBPort.PORT );
				addresses.add( address );
			}
		}
		catch ( Exception e )
		{
		}

		if ( addresses.isEmpty( ) )
		{
			return;
		}

		if ( addresses.isEmpty( ) )
		{
			return;
		}

		// Mongo options
		Builder builder = new MongoClientOptions.Builder( );
		builder.connectionsPerHost( 10 );
		// builder.threadsAllowedToBlockForConnectionMultiplier( 0 );
		builder.connectTimeout( 10000 );
		builder.autoConnectRetry( false );
		builder.maxWaitTime( 10000 );
		builder.socketTimeout( 10000 );
		MongoClientOptions options = builder.build( );

		MongoClient client = new MongoClient( addresses, options );
		// DB instance
		db = client.getDB( dbName );
		db.setWriteConcern( WriteConcern.NORMAL );

		boolean auth = false;
		if ( dbUserName != null && dbUserName.length( ) > 0 )
		{
			if ( !auth )
			{
				db.authenticate( dbUserName, dbPassword == null
						? null
						: dbPassword.toCharArray( ) );
			}
		}
	}

	private static void girdfstest( ) throws IOException
	{

		GridFS gridFS = new GridFS( db );
		File file = new File( "C:\\Users\\cheng\\Desktop\\a.txt" );
		GridFSInputFile gfsf = gridFS.createFile( file );

		long timeMil = System.currentTimeMillis( );
		Date date = new Date( );
		String contentType = "html";
		String extension = "." + contentType;
		gfsf.put( "contentType", contentType );
		gfsf.put( "filename", timeMil + extension );
		gfsf.put( "date", format.format( date ) );
		gfsf.put( "time", (int) ( date.getTime( ) / 1000 ) );

		BasicDBObject parameter = new BasicDBObject( );
		parameter.append( "key", "309001115200016" );
		BasicDBObject metadata = new BasicDBObject( );
		metadata.append( "parameter", parameter );

		gfsf.setMetaData( metadata );
		gfsf.save( );
	}

	private static void findDocs( )
	{
		DBCollection coll = db.getCollection( "movie" );
		DBCursor result = coll.find( );
		for ( DBObject dbobj : result )
		{
			System.out.println( dbobj );
		}
	}

	private static void insertDoc( )
	{
		try
		{

			BasicDBObject doc = new BasicDBObject( "name", "mongo" ).append(
					"info", new BasicDBObject( "version", "3.2" ) );

			DBCollection coll = db.getCollection( "clo_2" );
			DBCursor cursor = coll.find( ).limit( 10 );
			try
			{
				while ( cursor.hasNext( ) )
				{
					System.out.println( cursor.next( ) );
				}
			}
			finally
			{
				cursor.close( );
			}

			// BasicDBObject query0 = new BasicDBObject("i", 70);
			// BasicDBObject up = new BasicDBObject("$set", new
			// BasicDBObject("i", 100));
			// coll.update(query0, up);

			// BasicDBObject query0 = new BasicDBObject("i", 100);
			// coll.remove(query0);

			// BasicDBObject query0 = new BasicDBObject("i", 70);
			// coll.insert(query0);
			//
			// BasicDBObject query = new BasicDBObject("i", new
			// BasicDBObject("$gt",50));
			// DBCursor cursor = coll.find(query);
			// try {
			// while(cursor.hasNext()) {
			// System.out.println(cursor.next());
			// }
			// } finally {
			// cursor.close();
			// }

		}
		catch ( Exception e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace( );
		}
	}

}

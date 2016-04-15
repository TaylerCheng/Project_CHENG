
package com.cg.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaiyuandao.loan.fs.FileManager;
import com.kaiyuandao.loan.fs.file.FileEntry;

public class KyloanFStest
{

	protected static Logger logger = LoggerFactory
			.getLogger( KyloanFStest.class );

	public static FileManager fm = null;

	@BeforeClass
	public static void setUp( ) throws Exception
	{
		fm = new FileManager( );
	}

	@Test
	public void test001saveFile( )
	{
		InputStream in = null;
		try
		{
			in = new BufferedInputStream( new FileInputStream( "D:/intel.mp4" ) );
			String filePath = "/vedio/intel.mp4";
			FileEntry vfile = new FileEntry( filePath, in );
			assertTrue( 1 == fm.saveFile( vfile ) );
		}
		catch ( Exception e )
		{
			assertTrue( false );
		}
		finally
		{
			if ( in != null )
			{
				try
				{
					in.close( );
				}
				catch ( IOException e )
				{
					e.printStackTrace( );
				}
			}
		}
	}

	@Test
	public void test002getFile( )
	{
		InputStream in = null;
		OutputStream out = null;
		FileEntry vfile = null;
		try
		{
			String filePath = "/video/intel.mp4";
			vfile = fm.getFile( filePath );

			int count = 0;
			if ( vfile != null )
			{
				in = vfile.getInputStream( );
				out = new FileOutputStream( new File(
						"C:/Users/cheng/Desktop/intel.mp4" ) );

				byte[] buf = new byte[1024];
				int len = 0;
				while ( ( len = in.read( buf ) ) > 0 )
				{
					out.write( buf, 0, len );
					count += len;
				}
				out.flush( );
			}
			assertTrue( count == vfile.getSize( ) );
		}
		catch ( Exception e )
		{
			e.printStackTrace( );
			assertTrue( false );
		}
		finally
		{
			if ( out != null )
			{
				try
				{
					out.close( );
				}
				catch ( IOException e )
				{
					e.printStackTrace( );
				}
			}
			if ( vfile != null )
			{
				try
				{
					vfile.close( );
				}
				catch ( Exception e )
				{
					e.printStackTrace( );
				}
			}
		}

	}
}
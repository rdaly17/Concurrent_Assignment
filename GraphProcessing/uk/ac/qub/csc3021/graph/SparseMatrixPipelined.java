package uk.ac.qub.csc3021.graph;

import java.io.File;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// This class represents the adjacency matrix of a graph as a sparse matrix
// in coordinate format (COO)
public class SparseMatrixPipelined extends SparseMatrix {
    // TODO: variable declarations
    // ...
    int num_vertices; // Number of vertices in the graph
    int num_edges;    // Number of edges in the graph
    int[] sources;
    int[] destinations;
    //buffer used for passing information between producer and consumer
    private final BlockingQueue<int[]> buffer;
    //initiate an extra buffered reader that can be used by our new edgemap method
    private BufferedReader rd1;
    private BufferedReader rd2;
    
    
    
    public SparseMatrixPipelined( String file, int bufferSize ) {
    buffer = new LinkedBlockingQueue<>(bufferSize);
	rd1 = createBufferedReader(file);
	rd2 = createBufferedReader(file);
	try {
		readFile(rd1);
	} catch (Exception e) {
		System.err.println("Exception, " + e);
	}
    }
    
    //create buffered reader method so our buffered readers are available 
    //to our readFile and edge map methods
    private BufferedReader createBufferedReader(String file) {
    	try {
            FileInputStream fileInputStream = new FileInputStream(file);
            InputStreamReader is = new InputStreamReader(fileInputStream, "UTF-8");
            return new BufferedReader(is);
        } catch( FileNotFoundException e ) {
    	    System.err.println( "File not found: " + e );
    	    return null;
    	} catch( UnsupportedEncodingException e ) {
    	    System.err.println( "Unsupported encoding exception: " + e );
    	    return null;
    	} catch( Exception e ) {
    	    System.err.println( "Exception: " + e );
    	    return null;
    	}
    }
    

    int getNext( BufferedReader rd ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	return Integer.parseInt( line );
    }

    void getNextPair( BufferedReader rd, int pair[] ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	StringTokenizer st = new StringTokenizer( line );
	pair[0] = Integer.parseInt( st.nextToken() );
	pair[1] = Integer.parseInt( st.nextToken() );
    }

    void readFile( BufferedReader rd ) throws Exception {
	String line = rd.readLine();
	if( line == null )
	    throw new Exception( "premature end of file" );
	if( !line.equalsIgnoreCase( "COO" ) )
	    throw new Exception( "file format error -- header" );
	
	num_vertices = getNext(rd);
	num_edges = getNext(rd);

	// TODO: Allocate memory for the COO representation
	// ...
	sources = new int[num_edges];
	destinations = new int[num_edges];
	
	

	int edge[] = new int[2];
	for( int i=0; i < num_edges; ++i ) {
	    getNextPair( rd, edge );
	    // TODO:
	    //    Insert edge with source edge[0] and destination edge[1]
	    // ...
	    
	    sources[i] = edge[0];
	    destinations[i] = edge[1];
	    
	}
    }

    // Return number of vertices in the graph
    public int getNumVertices() { return num_vertices; }

    // Return number of edges in the graph	
    public int getNumEdges() { return num_edges; }

    // Auxiliary function for PageRank calculation
    public void calculateOutDegree( int outdeg[] ) {
	// TODO:
	//    Calculate the out-degree for every vertex, i.e., the
	//    number of edges where a vertex appears as a source vertex.
	// ...
    	for (int i = 0; i < num_edges; i++) {
    		outdeg[sources[i]]++;
    	}
   	
    	
    }
    
    //wrap the relax call into a method for a cleaner edge map method
    private void processEdge(Relax relax, int[] edge) {
        relax.relax(edge[0], edge[1]);
    }
    
    public void edgemap(Relax relax) {
        try {
            Thread producerThread = new Thread(() -> {
                try {
                	//skip the file name, number of vertices and number of edges
                	rd2.readLine();
                	rd2.readLine();
                	rd2.readLine();
                	//read in our edges in blocks of 128
                    int edge[] = new int[2];
                    int edge_block[][] = new int[256][2];
                    int edge_blockIndex = 0;

                    for (int i = 0; i < num_edges; ++i) {
                        getNextPair(rd2, edge);
                        edge_block[edge_blockIndex++] = edge.clone();

                        if (edge_blockIndex == 256) {
                            for (int[] array : edge_block) {
                                buffer.put(array);
                            }
                            edge_block = new int[256][2];
                            edge_blockIndex = 0;
                        }
                    }

                    //last block may not be a complete block of 128
                    if (edge_blockIndex > 0) {
                        for (int i = 0; i < edge_blockIndex; i++) {
                            buffer.put(edge_block[i]);
                        }
                    }
                    //end the process
                    buffer.put(new int[0]);
                } catch (Exception e) {
                    System.err.println("Exception in producer: " + e);
                }
            });

            Thread consumerThread = new Thread(() -> {
                try {
                    while (true) {
                    	//process starts once producer has ended
                        int[] edge = buffer.take();
                        if (edge.length == 0) {
                            break;
                        }
                        processEdge(relax, edge);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            
            //start the threads
            producerThread.start();
            consumerThread.start();

            producerThread.join();
            consumerThread.join();
        } catch (Exception e) {
            System.err.println("Exception: " + e);
        } finally {
            try {
                if (rd2 != null) {
                    rd2.close();
                }
            } catch (Exception e) {
                System.err.println("Exception : " + e);
            }
        }
    }

    public void ranged_edgemap( Relax relax, int from, int to ) {
	// Only implement for parallel/concurrent processing
	// if you find it useful. Not relevant for the fist assignment.
    }
}

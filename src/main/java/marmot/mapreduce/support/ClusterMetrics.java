package marmot.mapreduce.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterMetrics {
	@SuppressWarnings("unused")
	private final static Logger s_logger = LoggerFactory.getLogger(ClusterMetrics.class);
	
	private final String m_rmAddress;
	
	private int m_nodeCount;
	private int m_activeNodeCount;
	private int m_vcoreCount;
	private List<ClusterNodeInfo> m_clusterNodes;
	
	public static ClusterMetrics get(String rmAddress) {
		return new ClusterMetrics(rmAddress);
	}
	
	private ClusterMetrics(String rmAddress) {
		m_rmAddress = rmAddress;
		
		update();
	}
	
	public void update() {
		Map<String,Object> metrics = getClusterMetrics(m_rmAddress);
		m_nodeCount = ((Long)metrics.get("totalNodes")).intValue();
		m_activeNodeCount = ((Long)metrics.get("activeNodes")).intValue();
		m_vcoreCount = ((Long)metrics.get("totalVirtualCores")).intValue();
		
		JSONArray nodes = (JSONArray)getClusterNodes(m_rmAddress).get("node");
		m_clusterNodes = FStream.from(nodes)
								.map(node -> toClusterNodeInfo((JSONObject)node))
								.toList();
	}
	
	public int getNodeCount() {
		return m_nodeCount;
	}
	
	public ClusterMetrics setNodeCount(int count) {
		m_nodeCount = count;
		return this;
	}
	
	public int getActiveNodeCount() {
		return m_activeNodeCount;
	}
	
	public int getTotalVCoreCount() {
		return m_vcoreCount;
	}
	
	public List<ClusterNodeInfo> listClusterNodeInfos() {
		return Collections.unmodifiableList(m_clusterNodes);
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String,Object> getClusterMetrics(String address) {
		String url = String.format("http://%s/ws/v1/cluster/metrics", address);
				
		DefaultHttpClient client = new DefaultHttpClient();
		HttpGet getRequest = new HttpGet(url);
		getRequest.addHeader("accept", "application/json");
		
		HttpResponse response;
		try {
			response = client.execute(getRequest);
			if ( response.getStatusLine().getStatusCode() != 200 ) {
				throw new IOException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}
			
			InputStreamReader resource = new InputStreamReader((response.getEntity().getContent()),
																StandardCharsets.UTF_8);
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject)parser.parse(new BufferedReader(resource));
			JSONObject metrics = (JSONObject)jsonObject.get("clusterMetrics");
			
			return (Map<String,Object>)metrics;
		}
		catch ( IOException | ParseException e ) {
			throw new RuntimeException("fails to get metrics from " + url);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Map<String,Object> getClusterNodes(String address) {
		String url = String.format("http://%s/ws/v1/cluster/nodes", address);
				
		DefaultHttpClient client = new DefaultHttpClient();
		HttpGet getRequest = new HttpGet(url);
		getRequest.addHeader("accept", "application/json");
		
		HttpResponse response;
		try {
			response = client.execute(getRequest);
			if ( response.getStatusLine().getStatusCode() != 200 ) {
				throw new IOException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}
			
			InputStreamReader resource = new InputStreamReader((response.getEntity().getContent()),
																StandardCharsets.UTF_8);
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject)parser.parse(new BufferedReader(resource));
			JSONObject metrics = (JSONObject)jsonObject.get("nodes");
			
			return (Map<String,Object>)metrics;
		}
		catch ( IOException | ParseException e ) {
			throw new RuntimeException("fails to get metrics from " + url);
		}
	}
	
	private ClusterNodeInfo toClusterNodeInfo(JSONObject node) {
		ClusterNodeInfo.State state = ClusterNodeInfo.State.valueOf((String)node.get("state"));
		long avail = (long)node.get("availableVirtualCores");
		long used = (long)node.get("usedVirtualCores");
		
		return new ClusterNodeInfo((String)node.get("id"),
									(String)node.get("rack"),
									(String)node.get("nodeHostName"),
									state,
									(long)node.get("numContainers"),
									(avail+used));
	}
}

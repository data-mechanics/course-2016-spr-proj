/***********************************

@author Sofia Maria Nikolakaki <smnikol@bu.edu>
@edited by ben lawson <balawson@bu.edu>

***********************************/

package getintersections;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileInputStream; //added by Ben
import java.io.ObjectOutputStream; //added by Ben http://stackoverflow.com/questions/4738162/java-writting-reading-a-map-from-disk
import java.io.ObjectInputStream; //added by Ben
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays; //add by Ben (to print stringArray)
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.*;
import javax.xml.soap.Node;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class RoadNetwork {
	static private List<Long> refNodes;
	static private HashMap<Long, List<Long>> mapWayRef = new HashMap<Long, List<Long>>();
	static private HashMap<Long, String> mapNameRef = new HashMap<Long, String>();
	static private HashMap<Long, String> mapTypeRef = new HashMap<Long, String>();
	static private List<Long> roadIntersect;
	static private List<String> nameIntersect;
	static private List<String> typeIntersect;

	static private HashMap<Long, List<Long>> mapInterSectCodes = new HashMap<Long, List<Long>>();
	static private HashMap<Long, List<String>> mapInterSectNames = new HashMap<Long, List<String>>();
	static private HashMap<Long, List<String>> mapInterSectTypes = new HashMap<Long, List<String>>();

	static private HashMap<String, Integer> populationAssignment = new HashMap<String, Integer>();

	static private List<Long> nodeIds;
	static private List<Float> nodeLat;
	static private List<Float> nodeLong;

	static private List<Float> finalX;
	static private List<Float> finalY;

	static private List<Float> queryX;
	static private List<Float> queryY;

        static public final String path = "/home/jedidiah/dev/sandbox/course-2016-spr-proj-one/balawson/BostonNetwork";
        static public final String shortPath = "/home/jedidiah/dev/sandbox/course-2016-spr-proj-one/balawson";
        //static public final String path = "/home/abel/dev/sandbox/course-2016-spr-proj-one/balawson/BostonNetwork";
        //static public final String shortPath = "/home/abel/dev/sandbox/course-2016-spr-proj-one/balawson";

	/* IntersectionIDs function stores in separate lists the node ids, their respective latitude 
	 * and their respective longitude (3 lists) these lists are nodeIds, nodeLat and nodeLong. The 
	 * input is an osm file produced by the combination of Open Street Maps and Osmosis. It can also be 
	 * the direct osm file of a region downloaded from Open Street Map.*/
        
        	
	private static void IntersectionIDs() throws NumberFormatException, IOException {
		long tStart = System.currentTimeMillis();
		nodeIds = new ArrayList<Long>();
		nodeLat = new ArrayList<Float>();
		nodeLong = new ArrayList<Float>();
		FileReader input = new FileReader(path + "/src/nodeid.osm");
		BufferedReader bufRead = new BufferedReader(input);
		String myLine = null;

		System.out.println("Creating Node Ids with Respective Latitude and Longitude");
		long tStartid = System.currentTimeMillis();
		while ((myLine = bufRead.readLine()) != null) {
			String[] array = myLine.split("<node id=\"|\" version|lat=\"|\" lon=\"|\"/>|\" \"|\">");
			for (int i = 0; i < array.length; i++) {
				if (i == 1) {
					nodeIds.add(Long.parseLong(array[1]));
				} else if (i == 3) {
					nodeLat.add(Float.parseFloat(array[3]));
				} else if (i == 4) {
					nodeLong.add(Float.parseFloat(array[4]));
				} else {

				}
			}

		}
		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStartid;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("Elapsed Time to Create Node Ids with Respective Latitude and Longitude: " + elapsedSeconds
				+ " (sec)");
	}
        
	/* PersonQuery function considers each person's location a query location. 
	 * In order to do so it receives as input the xml file produced by the twitter data.
	 * It produces three lists the two of these (queryX and queryY) contain 
	 * the latitude and the longitude position of the person respectively.
	 * The third array personType contains the type of person to whom the position query corresponds to
	 * and can either be Tourist or Local. */

        //NOTE: this filters out posts that are at the exact same geocoordiate. These posts will be condensed into 1 post.
	private static void PersonQuery() throws NumberFormatException, IOException {
		long tStartid = System.currentTimeMillis();
		System.out.println("Creating the Locations of People in Boston");
		BufferedReader bufRead;
		String myLine = null;
		FileReader inputTourists = new FileReader(
                                  shortPath  + "/twitter.xml");
				//shortPath  + "/2015-12-28.xml");
				//shortPath  + "/sample2.xml");
		bufRead = new BufferedReader(inputTourists);
		myLine = null;
		String[] array = null;
		queryX = new ArrayList<Float>();
		queryY = new ArrayList<Float>();
		while ((myLine = bufRead.readLine()) != null) {
			array = myLine.split(
					" |loca|lng|lat|local|\">|</ROW>|<ROW|<?xml version=\"1.0\"?>|<ROWSET>|FIELD5=\"|FIELD6=\"|FIELD1=\"|FIELD2=\"|FIELD3=\"|FIELD4=\"|\" ");
                        //System.out.println(Arrays.toString(array)); 
			for (int i = 0; i < array.length; i++) {
				//if (array[i].equals("False") || array[i].equals("True")) {
                                        try {
					    if (queryY.contains(Float.parseFloat(array[7])) && queryX.contains(Float.parseFloat(array[9]))) {
						    if (!(queryY.indexOf(Float.parseFloat(array[7])) == queryX
								    .indexOf(Float.parseFloat(array[0])))) {
							    queryY.add(Float.parseFloat(array[7]));
							    queryX.add(Float.parseFloat(array[9]));
						    }
					    } else {
						    queryY.add(Float.parseFloat(array[7]));
						    queryX.add(Float.parseFloat(array[9]));
					    }
                                        }
                                        catch (Exception e) {
                                        } 
                                }

		}

		long tEndid = System.currentTimeMillis();
		long tDeltaid = tEndid - tStartid;
		double elapsedSecondsid = tDeltaid / 1000.0;
		System.out.println("Elapsed Time to Create the Locations of People in Boston: " + elapsedSecondsid
				+ " (sec)");
	}

	/* ExtractingRoads() uses the .osm files produced from the combination of OpenStreetMaps and Osmosis
	 * to create a single hash map called mapWayRef. mapWayRef is a hash map whose key is the id of the road
	 * and each entry contains a list with all the events that occur on the road. Among these events are also
	 * intersection but at this point we have not filtered the useful information 
	 */
	private static void ExtractingRoads() throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		System.out.println("Creating the hash map of Road Ids and Events that happen along them.");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();
		File waysInput = new File(path + "/src/roadmaps.osm");
		File motorInput = new File(path + "/src/motorway.osm");
		File motorLinkInput = new File(path + "/src/motorway_link.osm");
		File primInput = new File(path + "/src/primary.osm");
		File primLinkInput = new File(path + "/src/primary_link.osm");
		File secondInput = new File(path + "/src/secondary.osm");
		File secondLinkInput = new File(path + "/src/secondary_link.osm");
		File tertInput = new File(path + "/src/tertiary.osm");
		File tertLinkInput = new File(path + "/src/tertiary_link.osm");
		Document docWays = dBuilder.parse(waysInput);
		docWays.getDocumentElement().normalize();
		Document docMotor = dBuilder.parse(motorInput);
		docMotor.getDocumentElement().normalize();
		Document docMotorLink = dBuilder.parse(motorLinkInput);
		docMotorLink.getDocumentElement().normalize();
		Document docPrimInput = dBuilder.parse(primInput);
		docPrimInput.getDocumentElement().normalize();
		Document docPrimLinkInput = dBuilder.parse(primLinkInput);
		docPrimLinkInput.getDocumentElement().normalize();
		Document docSecondInput = dBuilder.parse(secondInput);
		docSecondInput.getDocumentElement().normalize();
		Document docSecondLinkInput = dBuilder.parse(secondLinkInput);
		docSecondLinkInput.getDocumentElement().normalize();
		Document docTertInput = dBuilder.parse(tertInput);
		docTertInput.getDocumentElement().normalize();
		Document docTertLinkInput = dBuilder.parse(tertLinkInput);
		docTertLinkInput.getDocumentElement().normalize();

		XPath xPath = XPathFactory.newInstance().newXPath();
		String expressionWays = "//osm/way"; // index starts from 1

		NodeList nodeListWays = (NodeList) xPath.compile(expressionWays).evaluate(docWays, XPathConstants.NODESET);
		NodeList nodeListWays2 = (NodeList) xPath.compile(expressionWays).evaluate(docMotor, XPathConstants.NODESET);
		NodeList nodeListWays3 = (NodeList) xPath.compile(expressionWays).evaluate(docMotorLink,
				XPathConstants.NODESET);
		NodeList nodeListWays4 = (NodeList) xPath.compile(expressionWays).evaluate(docPrimInput,
				XPathConstants.NODESET);
		NodeList nodeListWays5 = (NodeList) xPath.compile(expressionWays).evaluate(docPrimLinkInput,
				XPathConstants.NODESET);
		NodeList nodeListWays6 = (NodeList) xPath.compile(expressionWays).evaluate(docSecondInput,
				XPathConstants.NODESET);
		NodeList nodeListWays7 = (NodeList) xPath.compile(expressionWays).evaluate(docSecondLinkInput,
				XPathConstants.NODESET);
		NodeList nodeListWays8 = (NodeList) xPath.compile(expressionWays).evaluate(docTertInput,
				XPathConstants.NODESET);
		NodeList nodeListWays9 = (NodeList) xPath.compile(expressionWays).evaluate(docTertLinkInput,
				XPathConstants.NODESET);
		long tStart1 = System.currentTimeMillis();
		/* TODO remove comments to consider all residential roads
		 * experiment using 50 roads to evaluate results.
		 */
		//for (int i = 0; i < nodeListWays.getLength(); i++) { 
                
		for (int i = 0; i < 50; i++) { 
			org.w3c.dom.Node nNode = nodeListWays.item(i); // nodeListNodes
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				org.w3c.dom.Node eElement = nNode;
				String expression1 = "//osm/way[" + (i + 1) + "]/nd";					
				NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docWays, XPathConstants.NODESET);					
				refNodes = new ArrayList<Long>();
				
				for (int j = 0; j < nl.getLength(); j++) {
					org.w3c.dom.Node nNode2 = nl.item(j);
					org.w3c.dom.Node eElement2 = nNode2;
					refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
				}					
				String expression2 = "//osm/way[" + (i + 1) + "]/tag";
				NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docWays, XPathConstants.NODESET);
				for (int j = 0; j < nl1.getLength(); j++) {
					org.w3c.dom.Node nNode2_1 = nl1.item(j);
					org.w3c.dom.Node eElement2_1 = nNode2_1;
					if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
					{
						mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
					}
					if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
					{
						mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
					}
				}					
				mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);
			}
		}
		System.out.print(".");
		
		/* TODO remove comments to consider all kinds of roads
		 * in order to create complete map
		 */
		
		for (int i = 0; i < nodeListWays2.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays2.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docMotor, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long
								.parseLong(((org.w3c.dom.Element) eElement2)
										.getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docMotor, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
                
		for (int i = 0; i < nodeListWays3.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays3.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docMotorLink, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docMotorLink, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
                
		for (int i = 0; i < nodeListWays4.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays4.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docPrimInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docPrimInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
		for (int i = 0; i < nodeListWays5.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays5.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docPrimLinkInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docPrimLinkInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
		for (int i = 0; i < nodeListWays6.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays6.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docSecondInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docSecondInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
		for (int i = 0; i < nodeListWays7.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays7.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docSecondLinkInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docSecondLinkInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
		for (int i = 0; i < nodeListWays8.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays8.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docTertInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docTertInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.print(".");
		for (int i = 0; i < nodeListWays9.getLength(); i++) { // TODO
				org.w3c.dom.Node nNode = nodeListWays9.item(i); // nodeListNodes
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					org.w3c.dom.Node eElement = nNode;
					String expression1 = "//osm/way[" + (i + 1) + "]/nd";
					NodeList nl = (NodeList) xPath.compile(expression1).evaluate(docTertLinkInput, XPathConstants.NODESET);
					refNodes = new ArrayList<Long>();
					for (int j = 0; j < nl.getLength(); j++) {
						org.w3c.dom.Node nNode2 = nl.item(j);
						org.w3c.dom.Node eElement2 = nNode2;
						refNodes.add(Long.parseLong(((org.w3c.dom.Element) eElement2).getAttribute("ref").toString()));
					}
					String expression2 = "//osm/way[" + (i + 1) + "]/tag";
					NodeList nl1 = (NodeList) xPath.compile(expression2).evaluate(docTertLinkInput, XPathConstants.NODESET);
					for (int j = 0; j < nl1.getLength(); j++) {
						org.w3c.dom.Node nNode2_1 = nl1.item(j);
						org.w3c.dom.Node eElement2_1 = nNode2_1;
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("name"))
						{
							mapNameRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
						if(((org.w3c.dom.Element) eElement2_1).getAttribute("k").toString().equals("highway"))
						{
							mapTypeRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), ((org.w3c.dom.Element) eElement2_1).getAttribute("v"));
						}
					}		
					mapWayRef.put(Long.parseLong(((org.w3c.dom.Element) eElement).getAttribute("id").toString()), refNodes);

				}
			}
		System.out.println(".");
                //end comment HERE TODO
                
                /* save output to file so we don't have to run a bunch of times
                */

                FileOutputStream fos1 = new FileOutputStream("mapWayRef.ser");
                ObjectOutputStream oos1 = new ObjectOutputStream(fos1);
                oos1.writeObject(RoadNetwork.mapWayRef); 
                oos1.close();

                FileOutputStream fos2 = new FileOutputStream("mapNameRef.ser");
                ObjectOutputStream oos2 = new ObjectOutputStream(fos2);
                oos2.writeObject(RoadNetwork.mapNameRef); 
                oos2.close();

                FileOutputStream fos3 = new FileOutputStream("mapTypeRef.ser");
                ObjectOutputStream oos3 = new ObjectOutputStream(fos3);
                oos3.writeObject(RoadNetwork.mapTypeRef); 
                oos3.close();


		long tEnd1 = System.currentTimeMillis();
		long tDelta1 = tEnd1 - tStart1;
		double elapsedSeconds1 = tDelta1 / 1000.0;
		System.out.println(
				"Elapsed Time to Create the hash map of Road Ids and Events that happen along them: " + elapsedSeconds1 + " (sec)");
	}

        private static void LoadExtractedRoads() throws FileNotFoundException, IOException, ClassNotFoundException{
		System.out.println( "Loading hash map of Road Ids and Events that happend along them");
                FileInputStream fis1 = new FileInputStream("mapWayRef.ser");
                ObjectInputStream ois1 = new ObjectInputStream(fis1);
                RoadNetwork.mapWayRef = (HashMap) ois1.readObject(); 
                ois1.close();

                FileInputStream fis2 = new FileInputStream("mapNameRef.ser");
                ObjectInputStream ois2 = new ObjectInputStream(fis2);
                RoadNetwork.mapNameRef = (HashMap) ois2.readObject(); 
                ois2.close();

                FileInputStream fis3 = new FileInputStream("mapTypeRef.ser");
                ObjectInputStream ois3 = new ObjectInputStream(fis3);
                RoadNetwork.mapTypeRef = (HashMap) ois3.readObject(); 
                ois3.close();
        }

	/* RoadIntersections() finds the intersections between different roads. In order to do so it compares the
	 * event lists between two roads and if it identifies that two roads share the same event this means that an 
	 * intersection is found. The complete information about the intersection is stored in three separate hash
	 * maps. The key of each hash map is the intersection's id number. The hash maps store the name of the roads
	 * , the type of the roads and the road ids that form the intersection. 
	 */
	private static void RoadIntersections() throws FileNotFoundException, IOException{
		Long keyH = (long) 0;
		System.out.println("Finding intersections between roads.");
		long tStart2 = System.currentTimeMillis();

		for (Entry<Long, List<Long>> entry : mapWayRef.entrySet()) {
			List<Long> val = entry.getValue();
			for (int i = 0; i < val.size(); i++) {
				for (Entry<Long, List<Long>> entryN : mapWayRef.entrySet()) {
					List<Long> val2 = entryN.getValue();
					for (int j = 0; j < val2.size(); j++) {
						// Find the id and for this key get the respective
						// name and type from the hash maps of name and type
						// and that is it
						if (((long) val.get(i) == val2.get(j)) && entry.getKey() != entryN.getKey()) {
							keyH = entryN.getKey();
							String roadName = mapNameRef.get(keyH);
							String roadType = mapTypeRef.get(keyH);
							if (mapInterSectCodes.get(val.get(i)) != null) {
								if (!mapInterSectCodes.get(val.get(i)).contains(keyH)) {
									if (mapInterSectNames.get(val.get(i)) != null) {
										mapInterSectNames.get(val.get(i)).add(roadName);
									} else {
										nameIntersect = new ArrayList<String>();
										nameIntersect.add(roadName);
										mapInterSectNames.put(val.get(i), nameIntersect);
									}
									if (mapInterSectTypes.get(val.get(i)) != null) {
										mapInterSectTypes.get(val.get(i)).add(roadType);
									} else {
										typeIntersect = new ArrayList<String>();
										typeIntersect.add(roadType);
										mapInterSectTypes.put(val.get(i), typeIntersect);
									}
									mapInterSectCodes.get(val.get(i)).add(keyH);
								}
							} else {
								if (mapInterSectNames.get(val.get(i)) != null) {
									mapInterSectNames.get(val.get(i)).add(roadName);
								} else {
									nameIntersect = new ArrayList<String>();
									nameIntersect.add(roadName);
									mapInterSectNames.put(val.get(i), nameIntersect);
								}
								if (mapInterSectTypes.get(val.get(i)) != null) {
									mapInterSectTypes.get(val.get(i)).add(roadType);
								} else {
									typeIntersect = new ArrayList<String>();
									typeIntersect.add(roadType);
									mapInterSectTypes.put(val.get(i), typeIntersect);
								}
								roadIntersect = new ArrayList<Long>();
								roadIntersect.add(keyH);
								mapInterSectCodes.put(val.get(i), roadIntersect);
							}
						}
					}
				}
			}
		}

		long tEnd2 = System.currentTimeMillis();
		long tDelta2 = tEnd2 - tStart2;
		double elapsedSeconds2 = tDelta2 / 1000.0;
		System.out.println("Elapsed Time to Find Intersections between Roads: "
				+ elapsedSeconds2 + " (sec)");


                FileOutputStream fos1 = new FileOutputStream("mapInterSectCodes.ser");
                ObjectOutputStream oos1 = new ObjectOutputStream(fos1);
                oos1.writeObject(RoadNetwork.mapInterSectCodes); 
                oos1.close();

                FileOutputStream fos2 = new FileOutputStream("mapInterSectNames.ser");
                ObjectOutputStream oos2 = new ObjectOutputStream(fos2);
                oos2.writeObject(RoadNetwork.mapInterSectNames); 
                oos2.close();

                FileOutputStream fos3 = new FileOutputStream("mapInterSectTypes.ser");
                ObjectOutputStream oos3 = new ObjectOutputStream(fos3);
                oos3.writeObject(RoadNetwork.mapInterSectTypes); 
                oos3.close();

		try {
			PrintStream out = new PrintStream(new FileOutputStream(path + "/outputs/intersectionsOutput.txt"));
			for (Entry<Long, List<Long>> entry : mapInterSectCodes.entrySet()) {
				List<Long> val = entry.getValue();
				Long key = entry.getKey();
				out.println("Node In XML: " + key + " with Road Intersections: " + val);
			}
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		int retrieveEl = 0;
		int nodeIndex = 0;

		finalX = new ArrayList<Float>();
		finalY = new ArrayList<Float>();

		for (Entry<Long, List<Long>> entry : mapInterSectCodes.entrySet()) {
			List<Long> val = entry.getValue();
			Long key = entry.getKey();

			for (int i = 0; i < val.size(); i++) {
				for (int j = 0; j < nodeIds.size(); j++) {
					if (key.equals(nodeIds.get(j))) {
						nodeIndex = j;
						break;
					}
				}
			}
			// intersect2D.add(assignPoint);
			finalX.add(retrieveEl, nodeLat.get(nodeIndex));
			finalY.add(retrieveEl, nodeLong.get(nodeIndex));
			retrieveEl++;
		}
	}

        private static void LoadIntersections() throws FileNotFoundException, IOException, ClassNotFoundException{
		System.out.println( "Loading hash map of Intersections");
                
                FileInputStream fis1 = new FileInputStream("mapInterSectCodes.ser");
                ObjectInputStream ois1 = new ObjectInputStream(fis1);
                RoadNetwork.mapInterSectCodes = (HashMap) ois1.readObject(); 
                ois1.close();

                FileInputStream fis2 = new FileInputStream("mapInterSectNames.ser");
                ObjectInputStream ois2 = new ObjectInputStream(fis2);
                RoadNetwork.mapInterSectNames = (HashMap) ois2.readObject(); 
                ois2.close();

                FileInputStream fis3 = new FileInputStream("mapInterSectTypes.ser");
                ObjectInputStream ois3 = new ObjectInputStream(fis3);
                RoadNetwork.mapInterSectTypes = (HashMap) ois3.readObject(); 
                ois3.close();
                
		int retrieveEl = 0;
		int nodeIndex = 0;

		finalX = new ArrayList<Float>();
		finalY = new ArrayList<Float>();

		for (Entry<Long, List<Long>> entry : mapInterSectCodes.entrySet()) {
			List<Long> val = entry.getValue();
			Long key = entry.getKey();

			for (int i = 0; i < val.size(); i++) {
				for (int j = 0; j < nodeIds.size(); j++) {
					if (key.equals(nodeIds.get(j))) {
						nodeIndex = j;
						break;
					}
				}
			}
			// intersect2D.add(assignPoint);
			finalX.add(retrieveEl, nodeLat.get(nodeIndex));
			finalY.add(retrieveEl, nodeLong.get(nodeIndex));
			retrieveEl++;
                }
        }
	/* AssignPeopleToNodes() assigns people to nodes/intersections. Given the query location of a person
	 * this function finds the shortest intersection location and assigns that person to that intersection.
	 * It stores in touristAssignment and localAssignment hash maps the number of locals and tourists for a given
	 * location. The key of this hash maps is the concatenation of the latitude and longitude of the intersection.
	 */
	
	private static void AssignPeopleToNodes(){
		long tStart4 = 0;
		double distance = 0;
		double minDistance = Double.MAX_VALUE;
		double x = 0;
		double y = 0;
		System.out.println("Assigning People(Tourists/Locals to Intersections");
		Point2D basePoint = new Point2D.Double(0, 0);
		Point2D queryPoint = new Point2D.Double(0, 0);
		String keyM = "";
		for (int j = 0; j < queryX.size(); j++) {
			queryPoint = new Point2D.Double(queryX.get(j), queryY.get(j));
			for (int i = 0; i < finalX.size(); i++) {
				basePoint.setLocation(finalX.get(i), finalY.get(i));
				distance = basePoint.distance(queryPoint);
				if (distance < minDistance) {
					minDistance = distance;
					x = basePoint.getX();
					y = basePoint.getY();
				}
			}
			keyM = Float.toString((float) x) + Float.toString((float) y);
	                if (populationAssignment.containsKey(keyM)) {
		            int num = populationAssignment.get(keyM);
		            num = num + 1;
                            populationAssignment.put(keyM, num);
			} else {
			    populationAssignment.put(keyM, 1);
                        }
			minDistance = Double.MAX_VALUE;
		}
		long tEnd4 = System.currentTimeMillis();
		long tDelta4 = tEnd4 - tStart4;
		double elapsedSeconds4 = tDelta4 / 1000.0;
		System.out.println("Elapsed Time to Assign People to Intersections: " + elapsedSeconds4 + " (sec)");
	}
	
	/*CreateFinalNetwork() creates the final road network of Boston - nodes are intersections and edges are roads
	 *each intersection also has attributes which show the number of tourists and locals assigned
	 *to this intersection
	 */
	private static void CreateFinalNetwork() throws ParserConfigurationException, TransformerException{
		System.out.println("Creating XML Boston Network");
		long tStart3 = System.currentTimeMillis();
		int retrieveEl = 0;
		int nodeIndex = 0;
		int index = 0;
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.newDocument();
		org.w3c.dom.Element rootElement = doc.createElement("graph");
		doc.appendChild(rootElement);
		org.w3c.dom.Element intersectElement = doc.createElement("intersection");
		org.w3c.dom.Element road = doc.createElement("road");
		org.w3c.dom.Element numPopulation = doc.createElement("population");
		for (Entry<Long, List<Long>> entry : mapInterSectCodes.entrySet()) {
			List<Long> val = entry.getValue();
			Long key = entry.getKey();
			List<String> valName = mapInterSectNames.get(key);
			List<String> valType = mapInterSectTypes.get(key);
			for (int i = 0; i < val.size(); i++) {
				// root element is intersections -- nodes of graph
				Attr id = doc.createAttribute("id");
				id.setValue(key.toString());
				intersectElement.setAttributeNode(id);
				for (int j = 0; j < nodeIds.size(); j++) {
					if (key.equals(nodeIds.get(j))) {
						nodeIndex = j;
						break;
					}
				}
				String keyF = " ";

				Attr lat = doc.createAttribute("lat");
				lat.setValue(nodeLat.get(nodeIndex).toString());
				intersectElement.setAttributeNode(lat);
				Attr lon = doc.createAttribute("long");
				lon.setValue(nodeLong.get(nodeIndex).toString());
				keyF = nodeLat.get(nodeIndex).toString() + nodeLong.get(nodeIndex).toString();
				intersectElement.setAttributeNode(lon);
				intersectElement.appendChild(road);

				Attr ref = doc.createAttribute("ref");
				ref.setValue(val.get(i).toString());
				road.setAttributeNode(ref);
				Attr name = doc.createAttribute("name");
				name.setValue(valName.get(i));
				road.setAttributeNode(name);
				Attr type = doc.createAttribute("type");
				type.setValue(valType.get(i));
				road.setAttributeNode(type);
				road = doc.createElement("road");

                                Attr num = doc.createAttribute("num");
				if (populationAssignment.containsKey(keyF)) {
					num.setValue(populationAssignment.get(keyF).toString());
				} else {
					num.setValue("0");
                                }
				numPopulation.setAttributeNode(num);
				intersectElement.appendChild(numPopulation);

				index++;

			}
			// intersect2D.add(assignPoint);
			finalX.add(retrieveEl, nodeLat.get(nodeIndex));
			finalY.add(retrieveEl, nodeLong.get(nodeIndex));

			rootElement.appendChild(intersectElement);
			intersectElement = doc.createElement("intersection");
			//numtourists = doc.createElement("numtourists");
			//numlocals = doc.createElement("numlocals");
			numPopulation = doc.createElement("population");
			retrieveEl++;
		}
		// write the content into xml file
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(path + "/outputs/roadnetwork.xml"));
		transformer.transform(source, result);
		long tEnd3 = System.currentTimeMillis();
		long tDelta3 = tEnd3 - tStart3;
		double elapsedSeconds3 = tDelta3 / 1000.0;
		System.out.println("Elapsed Time to Create XML Boston Network: " + elapsedSeconds3 + " (sec)");
	}
	public static void main(String[] args) throws NumberFormatException, IOException, XPathExpressionException, ParserConfigurationException, SAXException, TransformerException , ClassNotFoundException{
	
		RoadNetwork.IntersectionIDs();
		RoadNetwork.PersonQuery();
                File f = new File("mapNameRef.ser"); //check for cached version
                if (f.exists() && !f.isDirectory()){
                     RoadNetwork.LoadExtractedRoads();
                } else {
		     RoadNetwork.ExtractingRoads();
                }
                File f2 = new File("mapInterSectNames.ser"); //check for cached version
                if (f2.exists() && !f2.isDirectory()){
                     RoadNetwork.LoadIntersections();
                } else {
		RoadNetwork.RoadIntersections();
                }
		RoadNetwork.AssignPeopleToNodes();
		RoadNetwork.CreateFinalNetwork();
	}
}

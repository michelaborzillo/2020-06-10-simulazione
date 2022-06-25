package it.polito.tdp.imdb.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.GraphIterator;

import it.polito.tdp.imdb.db.Adiacenze;
import it.polito.tdp.imdb.db.ImdbDAO;

public class Model {
	ImdbDAO dao;
	Graph<Actor, DefaultWeightedEdge> grafo;
	Map<Integer, Actor> idMap;
	public Model () {
		dao= new ImdbDAO();
		idMap= new HashMap<Integer, Actor>();
	}

	public List<String> loadGeneri() {
		return dao.loadAllMovieGenre();
	}
	
	public void creaGrafo(String  genere) {
		this.grafo= new SimpleWeightedGraph<Actor, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		List<Actor> attori= dao.getVertici(genere, idMap);
		Graphs.addAllVertices(this.grafo, attori);
		for (Adiacenze a: dao.getArchi(idMap, genere)) {
			if (a.getPeso()!=0) 
				Graphs.addEdgeWithVertices(this.grafo, a.getA1(), a.getA2(), a.getPeso());
		}
		
	}
	
	
	public int nVertici() {
		return this.grafo.vertexSet().size();
		
	}

	public int nArchi() {
		return this.grafo.edgeSet().size();
	
	}
	
	public List<Actor> getVertici() {
		List<Actor> res= new ArrayList<Actor>(this.grafo.vertexSet());
		Collections.sort(res);
		return res;
	}
	
	public List<Actor> getVicini(Actor a) {
		List<Actor> result= new LinkedList<Actor>();
		GraphIterator<Actor, DefaultWeightedEdge> bfv = new BreadthFirstIterator<Actor, DefaultWeightedEdge>(this.grafo, a);
		while(bfv.hasNext()) {
			result.add(bfv.next());
			
		}
		
		Collections.sort(result);
		return result;
	}
	
	

}

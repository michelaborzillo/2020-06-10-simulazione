package it.polito.tdp.imdb.db;

import java.nio.InvalidMarkException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.imdb.model.Actor;
import it.polito.tdp.imdb.model.Director;
import it.polito.tdp.imdb.model.Movie;

public class ImdbDAO {
	
	public List<Actor> listAllActors(){
		String sql = "SELECT * FROM actors";
		List<Actor> result = new ArrayList<Actor>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor actor = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				
				result.add(actor);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Movie> listAllMovies(){
		String sql = "SELECT * FROM movies";
		List<Movie> result = new ArrayList<Movie>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Movie movie = new Movie(res.getInt("id"), res.getString("name"), 
						res.getInt("year"), res.getDouble("rank"));
				
				result.add(movie);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public List<Director> listAllDirectors(){
		String sql = "SELECT * FROM directors";
		List<Director> result = new ArrayList<Director>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Director director = new Director(res.getInt("id"), res.getString("first_name"), res.getString("last_name"));
				
				result.add(director);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<String> loadAllMovieGenre() {
		String sql="SELECT DISTINCT mg.genre AS genere "
				+ "FROM movies_genres mg";
		List<String> result = new ArrayList<String>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				result.add(res.getString("genere"));
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	
	public List<Actor> getVertici(String genere, Map<Integer, Actor>idMap) {
		String sql="SELECT DISTINCT a.* "
				+ "FROM movies_genres mg, roles r, actors a "
				+ "WHERE a.id=r.actor_id AND r.movie_id=mg.movie_id AND mg.genre=?";
		List<Actor> result = new ArrayList<Actor>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genere);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor actor = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				
				idMap.put(actor.getId(), actor);
				result.add(actor);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	
	
	public int getPeso(String genere, Actor a1, Actor a2) {
		String sql="SELECT COUNT(mg1.movie_id) AS peso "
				+ "FROM movies_genres mg1, roles r1, roles r2 "
				+ "WHERE mg1.movie_id=r1.movie_id AND mg1.movie_id=r2.movie_id "
				+ "AND mg1.genre=? AND r1.actor_id=? AND r2.actor_id=? AND r1.movie_id=r2.movie_id "
				+ "GROUP BY r1.actor_id, r2.actor_id "
				+ "HAVING peso>'1'";
		int peso=0;
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genere);
			st.setInt(2, a1.getId());
			st.setInt(3, a2.getId());
			ResultSet res = st.executeQuery();
			if (res.next()) {

				peso=res.getInt("peso");
			}
			conn.close();
			return peso;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	
	public List<Adiacenze> getArchi(Map<Integer, Actor>idMap, String genere) {
		String sql="SELECT COUNT(mg1.movie_id) AS peso, r1.actor_id AS a1, r2.actor_id AS a2 "
				+ "FROM movies_genres mg1, roles r1, roles r2 "
				+ "WHERE mg1.movie_id=r1.movie_id AND mg1.movie_id=r2.movie_id "
				+ "AND mg1.genre=? AND r1.movie_id=r2.movie_id AND r1.actor_id>r2.actor_id "
				+ "GROUP BY r1.actor_id, r2.actor_id "
				+ "HAVING peso>='1'";
		List<Adiacenze> result= new ArrayList<Adiacenze>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genere);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor a1=idMap.get(res.getInt("a1"));
				Actor a2= idMap.get(res.getInt("a2"));
				int peso= res.getInt("peso");
				Adiacenze a= new Adiacenze(a1, a2, peso);
				result.add(a);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		
	}
	
}

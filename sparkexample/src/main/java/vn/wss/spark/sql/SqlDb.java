package vn.wss.spark.sql;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlDb {
	private static final Logger log = LogManager.getLogger(SqlDb.class);
	private static SqlDb _instance = null;

	private SqlDb() {
		log.info("create new instance");
	}

	public static SqlDb getInstance() {
		if (_instance == null) {
			_instance = new SqlDb();
		}
		return _instance;
	}

	public RecommendedItem saveRecommendedItem(long idItem,
			long idRecommendedItem, int order) {
		log.info("save item: " + idItem + ", recommended item: "
				+ idRecommendedItem);
		EntityManager entityManager = EntityManagerUtil.getEntityManager();
		RecommendedItem recommendedItem = null;
		try {
			entityManager.getTransaction().begin();
			recommendedItem = new RecommendedItem(idItem, idRecommendedItem,
					order);
			recommendedItem = entityManager.merge(recommendedItem);
			entityManager.getTransaction().commit();
		} catch (Exception e) {
			log.error("ex when save proper noun: " + idItem, e);
			// entityManager.getTransaction().rollback();
		} finally {
			if (entityManager != null) {
				if (entityManager.getTransaction().isActive())
					entityManager.getTransaction().rollback();
				entityManager.close();
			}
		}
		return recommendedItem;
	}

	public int deleteAllRecommendedItem() {
		EntityManager entityManager = EntityManagerUtil.getEntityManager();
		int res = 0;
		try {
			entityManager.getTransaction().begin();
			Query query = entityManager
					.createNamedQuery("RecommendedItem.removeAll");
			res = query.executeUpdate();
			entityManager.getTransaction().commit();
		} catch (Exception e) {
			log.error(e.toString());
			entityManager.getTransaction().rollback();
		} finally {
			if (entityManager.getTransaction().isActive())
				entityManager.getTransaction().rollback();
			entityManager.close();
		}
		return res;
	}

	public int getSize() {
		EntityManager entityManager = EntityManagerUtil.getEntityManager();
		List<?> res = null;
		try {
			Query query = entityManager
					.createNamedQuery("RecommendedItem.findAll");
			res = query.getResultList();
			// RecommendedItem recommendedWord = (RecommendedItem) res.get(0);
			log.info(res.size());
		} catch (Exception e) {
			log.error(e.toString());
		} finally {
			if (entityManager != null) {
				entityManager.close();
			}
		}
		return res.size();
	}

}

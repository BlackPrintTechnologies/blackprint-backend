import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from utils.async_utils import get_thread_pool, run_sync_in_executor
from module.properties.controller import PropertyController, UserPropertyController
from module.layers.controller import BrandController, TrafficController, PropertyLayerController
from module.user.controller import UsersController, UserQuestionareController
from module.search.controller import SavedSearchesController
from module.group.controller import GroupsController

logger = logging.getLogger(__name__)

class AppWarmup:
    def __init__(self):
        self.warmup_start_time = None
        self.warmup_end_time = None
        self.controllers_warmed = False
        self.db_connections_warmed = False
        self.thread_pool_warmed = False

    def warmup_thread_pool(self):
        """Pre-initialize the thread pool"""
        try:
            logger.info("🔥 Warming up thread pool...")
            pool = get_thread_pool()
            
            # Test the thread pool with a simple task
            def test_task():
                return "thread_pool_test"
            
            future = pool.submit(test_task)
            result = future.result(timeout=5)
            
            if result == "thread_pool_test":
                self.thread_pool_warmed = True
                logger.info("✅ Thread pool warmed successfully")
            else:
                logger.warning("⚠️ Thread pool test failed")
                
        except Exception as e:
            logger.error(f"❌ Thread pool warmup failed: {e}")

    def warmup_database_connections(self):
        """Pre-establish and test database connections"""
        try:
            logger.info("🔥 Warming up database connections...")
            
            # Test each controller's database connection
            controllers = [
                ("PropertyController", PropertyController()),
                ("UserPropertyController", UserPropertyController()),
                ("BrandController", BrandController()),
                ("TrafficController", TrafficController()),
                ("PropertyLayerController", PropertyLayerController()),
                ("UsersController", UsersController()),
                ("UserQuestionareController", UserQuestionareController()),
                ("SavedSearchesController", SavedSearchesController()),
                ("GroupsController", GroupsController())
            ]
            
            for name, controller in controllers:
                try:
                    if hasattr(controller, 'db'):
                        # Test database connection
                        connection = controller.db.connect()
                        if connection:
                            # Execute a simple test query to warm up the connection
                            cursor = connection.cursor()
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                            cursor.close()
                            controller.db.disconnect(connection)
                            logger.debug(f"✅ {name} database connection warmed")
                        else:
                            logger.warning(f"⚠️ {name} failed to connect to database")
                except Exception as e:
                    logger.warning(f"⚠️ {name} database warmup failed: {e}")
            
            self.db_connections_warmed = True
            logger.info("✅ Database connections warmed successfully")
            
        except Exception as e:
            logger.error(f"❌ Database connection warmup failed: {e}")

    def warmup_controllers(self):
        """Pre-load and initialize all controllers"""
        try:
            logger.info("🔥 Warming up controllers...")
            
            # Import and instantiate all controllers to load their modules
            controllers = {
                'property': PropertyController(),
                'user_property': UserPropertyController(),
                'brand': BrandController(),
                'traffic': TrafficController(),
                'property_layer': PropertyLayerController(),
                'users': UsersController(),
                'user_questionare': UserQuestionareController(),
                'saved_searches': SavedSearchesController(),
                'groups': GroupsController()
            }
            
            self.controllers_warmed = True
            logger.info(f"✅ {len(controllers)} controllers warmed successfully")
            
        except Exception as e:
            logger.error(f"❌ Controller warmup failed: {e}")

    async def warmup_async_operations(self):
        """Test async operations and executors"""
        try:
            logger.info("🔥 Warming up async operations...")
            
            # Test async executor with a sample operation
            def sample_sync_operation():
                time.sleep(0.01)  # Simulate small work
                return "async_test_complete"
            
            result = await run_sync_in_executor(sample_sync_operation)
            
            if result == "async_test_complete":
                logger.info("✅ Async operations warmed successfully")
            else:
                logger.warning("⚠️ Async operation test failed")
                
        except Exception as e:
            logger.error(f"❌ Async operation warmup failed: {e}")

    def warmup_query_plans(self):
        """Execute sample queries to warm up database query plans"""
        try:
            logger.info("🔥 Warming up database query plans...")
            
            # Execute sample queries that are commonly used
            pc = PropertyController()
            
            # Test a simple property query (this will cache the execution plan)
            try:
                connection = pc.db.connect()
                cursor = connection.cursor()
                
                # Sample queries to warm up plans (using LIMIT 1 to be fast)
                warmup_queries = [
                    "SELECT 1 as test_query",
                    "SELECT fid FROM blackprint_db_prd.data_product.v_parcel_v3 LIMIT 1",
                ]
                
                for query in warmup_queries:
                    try:
                        cursor.execute(query)
                        cursor.fetchone()
                        logger.debug(f"✅ Warmed query plan: {query[:50]}...")
                    except Exception as e:
                        logger.debug(f"⚠️ Query plan warmup failed for: {query[:50]}... - {e}")
                
                cursor.close()
                pc.db.disconnect(connection)
                logger.info("✅ Database query plans warmed successfully")
                
            except Exception as e:
                logger.warning(f"⚠️ Query plan warmup failed: {e}")
                
        except Exception as e:
            logger.error(f"❌ Query plan warmup failed: {e}")

    async def full_warmup(self):
        """Perform complete application warmup"""
        self.warmup_start_time = time.time()
        logger.info("🚀 Starting application warmup...")
        
        try:
            # Step 1: Warm up thread pool
            self.warmup_thread_pool()
            
            # Step 2: Warm up controllers (load modules)
            self.warmup_controllers()
            
            # Step 3: Warm up database connections
            self.warmup_database_connections()
            
            # Step 4: Warm up async operations
            await self.warmup_async_operations()
            
            # Step 5: Warm up query plans
            self.warmup_query_plans()
            
            self.warmup_end_time = time.time()
            warmup_duration = self.warmup_end_time - self.warmup_start_time
            
            logger.info(f"🎉 Application warmup completed in {warmup_duration:.2f} seconds")
            logger.info(f"📊 Warmup status - Thread Pool: {self.thread_pool_warmed}, "
                       f"DB Connections: {self.db_connections_warmed}, "
                       f"Controllers: {self.controllers_warmed}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Application warmup failed: {e}")
            return False

    def get_warmup_status(self):
        """Get current warmup status"""
        return {
            'thread_pool_warmed': self.thread_pool_warmed,
            'db_connections_warmed': self.db_connections_warmed,
            'controllers_warmed': self.controllers_warmed,
            'warmup_duration': (self.warmup_end_time - self.warmup_start_time) if self.warmup_end_time else None,
            'is_warmed_up': all([self.thread_pool_warmed, self.db_connections_warmed, self.controllers_warmed])
        }

# Global warmup instance
_app_warmup = AppWarmup()

def get_app_warmup():
    """Get the global warmup instance"""
    return _app_warmup

async def perform_startup_warmup():
    """Convenience function to perform startup warmup"""
    warmup = get_app_warmup()
    return await warmup.full_warmup() 
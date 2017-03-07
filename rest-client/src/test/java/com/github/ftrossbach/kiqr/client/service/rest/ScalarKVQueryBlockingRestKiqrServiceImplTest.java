package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.ConnectionException;
import com.github.ftrossbach.kiqr.client.service.QueryExecutionException;
import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Base64;
import java.util.Optional;

/**
 * Created by ftr on 07/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class ScalarKVQueryBlockingRestKiqrServiceImplTest {

    BlockingRestKiqrServiceImpl unitUnderTest = new BlockingRestKiqrServiceImpl("localhost", 4567);

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    String valueAsByteArray = Base64.getEncoder().encodeToString(Serdes.Long().serializer().serialize("", 42L));

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQuery.class, new KiqrCodec(ScalarKeyValueQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQueryResponse.class, new KiqrCodec(ScalarKeyValueQueryResponse.class));


    }


    @Test
    public void successfulQuery(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.reply(new ScalarKeyValueQueryResponse(valueAsByteArray));

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.assertTrue(scalarKeyValue.isPresent());
                context.assertEquals(42L, scalarKeyValue.get());
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void notFound(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(404, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.assertFalse(scalarKeyValue.isPresent());
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void badRequest(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(400, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {


                    Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                    context.fail();




            }, context.asyncAssertFailure(ex -> {

                context.assertTrue(ex instanceof IllegalArgumentException);
                async.complete();
            }));

        }));

    }

    @Test
    public void internalServerError(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(500, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {


                    Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                    context.fail();



            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof QueryExecutionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void connectionExceptionInvalidPort(TestContext context){


        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("localhost", -1);
                Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.fail();



            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void connectionExceptionInvalidHost(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("host with spaces", 4567);
                Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.fail();



            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void validButUnReachableHost(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("ihopethisdoesntexist", 4567);
                Optional<Long> scalarKeyValue = unitUnderTest.getScalarKeyValue("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.fail();



            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

}

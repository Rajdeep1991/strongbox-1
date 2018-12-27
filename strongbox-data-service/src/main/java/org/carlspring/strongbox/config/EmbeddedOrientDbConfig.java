package org.carlspring.strongbox.config;

import org.carlspring.strongbox.data.server.EmbeddedOrientDbServer;
import org.carlspring.strongbox.data.server.OrientDbServer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.orientechnologies.orient.core.db.OrientDB;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.zeroturnaround.zip.ZipUtil;

/**
 * @author Przemyslaw Fusik
 */
@Configuration
@Conditional(EmbeddedOrientDbConfig.class)
class EmbeddedOrientDbConfig
        extends CommonOrientDbConfig
        implements Condition
{

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedOrientDbConfig.class);

    @Bean(destroyMethod = "close")
    @DependsOn("orientDbServer")
    OrientDB orientDB()
            throws IOException
    {
        OrientDB orientDB = new OrientDB(StringUtils.substringBeforeLast(connectionConfig.getUrl(), "/"),
                                         connectionConfig.getUsername(),
                                         connectionConfig.getPassword(),
                                         getOrientDBConfig());
        String database = connectionConfig.getDatabase();

        if (!orientDB.exists(database))
        {
            logger.info(String.format("Unpacking database from snapshot [%s]...", database));

            try (InputStream is = new ClassPathResource("db/snapshot/strongbox-db-snapshot.zip").getInputStream())
            {
                ZipUtil.unpack(is, new File(EmbeddedOrientDbServer.getDatabasePath() + "/strongbox"));
            }
        }
        else
        {
            logger.info("Re-using existing database " + database + ".");
        }
        return orientDB;
    }

    @Bean
    OrientDbServer orientDbServer()
    {
        return new EmbeddedOrientDbServer();
    }

    @Override
    public boolean matches(ConditionContext conditionContext,
                           AnnotatedTypeMetadata metadata)

    {
        return ConnectionConfigOrientDB.resolveProfile(conditionContext.getEnvironment())
                                       .equals(ConnectionConfigOrientDB.PROFILE_EMBEDDED);
    }
}

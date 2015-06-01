/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.strategicgains.docussandra.persistence.impl;

import com.strategicgains.docussandra.domain.Document;
import com.strategicgains.docussandra.domain.Identifier;
import com.strategicgains.docussandra.domain.QueryResponseWrapper;
import com.strategicgains.docussandra.persistence.DocumentRepository;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author udeyoje
 */
@Deprecated
public class HadoopDocumentRepositoryImpl implements DocumentRepository
{

    private FileSystem fs;

    public HadoopDocumentRepositoryImpl()
    {
        try
        {
            Configuration config = new Configuration();
            config.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            config.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            fs = FileSystem.get(new URI("hdfs://localhost:54310"), config);
        } catch (IOException | URISyntaxException e)
        {
            throw new RuntimeException(e);//hacky! fix
        }
    }

    @Override
    public Document create(Document entity)
    {
        String table = entity.table().name();
        String database = entity.databaseName();
        FSDataOutputStream out = null;
        try
        {
            out = fs.create(new Path("hdfs://localhost:54310/hdocussandra/" + database + "/" + table, entity.getUuid().toString() + ".json"));
            out.writeBytes(entity.object().trim());
            return entity;
        } catch (IOException e)
        {
            throw new RuntimeException(e);//hacky; fix this
        } finally
        {
            if (out != null)
            {
                try
                {
                    out.close();
                } catch (IOException e)
                {
                    ;//don't care
                }
            }
        }

    }

    @Override
    public void delete(Document entity)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void delete(Identifier id)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean exists(Identifier identifier)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Document read(Identifier identifier)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public QueryResponseWrapper readAll(String database, String tableString, int limit, long offset)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String toString()
    {
        return super.toString(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Document update(Document entity)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

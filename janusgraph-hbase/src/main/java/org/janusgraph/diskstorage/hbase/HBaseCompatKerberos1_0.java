// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.security.UserGroupInformation;
import org.janusgraph.hadoop.kerberos.KerberosException;
import org.janusgraph.hadoop.kerberos.KerberosKeytabUser;
import org.janusgraph.hadoop.kerberos.KerberosUser;
import org.janusgraph.hadoop.kerberos.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicReference;

public class HBaseCompatKerberos1_0 implements HBaseCompat {

    public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";
    private  static Logger logger=LoggerFactory.getLogger(HBaseCompatKerberos1_0.class);
    private volatile UserGroupInformation ugi;
    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    @Override
    public void setCompression(HColumnDescriptor cd, String algorithm) {
        cd.setCompressionType(Compression.Algorithm.valueOf(algorithm));
    }

    @Override
    public HTableDescriptor newTableDescriptor(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        return new HTableDescriptor(tn);
    }

    @Override
    public ConnectionMask createConnection(Configuration conf) throws IOException
    {
        return new HConnection1_0(ConnectionFactory.createConnection(conf));
    }
    @Override
    public ConnectionMask createConnection(Configuration conf, String kerberosPrincipal, String kerberosKeytab) throws IOException, InterruptedException {
        return new HConnection1_0(this.getConnection(conf,kerberosPrincipal,kerberosKeytab));
    }
    private void setJaasFile(){
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String jaasPath=userdir + "jaas.conf";
        File file=new File(jaasPath);
        if(file.exists()){
            System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY,jaasPath);
            logger.info("设置jaas.conf文件路径->"+jaasPath);
        }else{
            throw new KerberosException("kerberos认证时需设置jaas.conf文件但在"+jaasPath+"目录下没有找到jaas.conf文件，请检查该目录下是否有jaas.conf文件并配置正确。");
        }
    }

    private void setkrb5File(){
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String krb5Path=userdir + "krb5.conf";
        File file=new File(krb5Path);
        if(file.exists()){
            System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY,krb5Path);
            logger.info("设置krb5.conf文件路径->"+krb5Path);
        }else{

            throw new KerberosException("kerberos认证时需设置krb5.conf文件但在"+krb5Path+"目录下没有找到krb5.conf文件，请检查该目录下是否有krb5.conf文件并配置正确。");
        }
    }

    protected Connection getConnection(Configuration conf,String principal,String keyTab) throws IOException, InterruptedException {
        // override with any properties that are provided
        if (SecurityUtil.isSecurityEnabled(conf)&& StringUtils.isNotBlank(principal)&&StringUtils.isNotBlank(keyTab)) {
            this.setkrb5File();
            this.setJaasFile();
            if (keyTab != null) {
                kerberosUserReference.set(new KerberosKeytabUser(principal, keyTab));
                logger.info("HBase Security Enabled, logging in as principal {} with keytab {}", new Object[] {principal, keyTab});
            }else {
                throw new IOException("Unable to authenticate with Kerberos, no keytab or password was provided");
            }
            ugi = SecurityUtil.getUgiForKerberosUser(conf, kerberosUserReference.get());
            logger.info("Successfully logged in as principal " + principal);

            return getUgi().doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return ConnectionFactory.createConnection(conf);
                }
            });
        } else {
            logger.info("正在使用....Simple Authentication");
            return ConnectionFactory.createConnection(conf);
        }
    }

    UserGroupInformation getUgi() {
        logger.trace("getting UGI instance");
        if (kerberosUserReference.get() != null) {
            KerberosUser kerberosUser = kerberosUserReference.get();
            logger.debug("kerberosUser is " + kerberosUser);
            try {
                logger.debug("checking TGT on kerberosUser [{}]", new Object[] {kerberosUser});
                kerberosUser.checkTGTAndRelogin();
            } catch (LoginException e) {
                throw new KerberosException("Unable to relogin with kerberos credentials for " + kerberosUser.getPrincipal(), e);
            }
        } else {
            logger.debug("kerberosUser was null, will not refresh TGT with KerberosUser");
        }
        return ugi;
    }

    @Override
    public void addColumnFamilyToTableDescriptor(HTableDescriptor tableDescriptor, HColumnDescriptor columnDescriptor)
    {
        tableDescriptor.addFamily(columnDescriptor);
    }

    @Override
    public void setTimestamp(Delete d, long timestamp)
    {
        d.setTimestamp(timestamp);
    }

}

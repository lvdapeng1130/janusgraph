package org.apache.janusgraph.spark.computer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.PersistResultGraphAware;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractJanusGraphComputer implements GraphComputer {

    private final static Pattern PATH_PATTERN =
        Pattern.compile(File.pathSeparator.equals(":") ? "([^:]|://)+" : ("[^" + File.pathSeparator + "]+"));

    protected final Logger logger;
    protected final JanusGraph janusGraph;
    protected boolean executed = false;
    protected final List<MapReduce> mapReducers = new ArrayList<>();
    protected VertexProgram<Object> vertexProgram;
    protected int workers = 1;

    protected ResultGraph resultGraph = null;
    protected Persist persist = null;

    protected GraphFilter graphFilter = new GraphFilter();

    public AbstractJanusGraphComputer(final JanusGraph janusGraph) {
        this.janusGraph = janusGraph;
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(final int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    protected void validateStatePriorToExecution() {
        // a graph computer can only be executed one time
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;
        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        // if the user didn't set desired persistence/resultgraph, then get from vertex program or else, no persistence
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        // determine persistence and result graph options
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist))
            throw Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);
        // if too many workers are requested, throw appropriate exception
        if (this.workers > this.features().getMaxWorkers())
            throw Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());
    }

    protected void loadJars(final Configuration hadoopConfiguration, final Object... params) {
        if (hadoopConfiguration.getBoolean(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String hadoopGremlinLibs = null == System.getProperty(Constants.HADOOP_GREMLIN_LIBS) ? System.getenv(Constants.HADOOP_GREMLIN_LIBS) : System.getProperty(Constants.HADOOP_GREMLIN_LIBS);
            if (null == hadoopGremlinLibs)
                this.logger.warn(Constants.HADOOP_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                try {
                    final Matcher matcher = PATH_PATTERN.matcher(hadoopGremlinLibs);
                    while (matcher.find()) {
                        final String path = matcher.group();
                        FileSystem fs;
                        try {
                            final URI uri = new URI(path);
                            fs = FileSystem.get(uri, hadoopConfiguration);
                        } catch (URISyntaxException e) {
                            fs = FileSystem.get(hadoopConfiguration);
                        }
                        final File file = AbstractJanusGraphComputer.copyDirectoryIfNonExistent(fs, path);
                        if (file.exists()) {
                            for (final File f : file.listFiles()) {
                                if (f.getName().endsWith(Constants.DOT_JAR)) {
                                    loadJar(hadoopConfiguration, f, params);
                                }
                            }
                        } else
                            this.logger.warn(path + " does not reference a valid directory -- proceeding regardless");
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }
    }

    protected abstract void loadJar(final Configuration hadoopConfiguration, final File file, final Object... params)
        throws IOException;

    @Override
    public Features features() {
        return new Features();
    }

    public class Features implements GraphComputer.Features {

        @Override
        public boolean supportsVertexAddition() {
            return false;
        }

        @Override
        public boolean supportsVertexRemoval() {
            return false;
        }

        @Override
        public boolean supportsVertexPropertyRemoval() {
            return false;
        }

        @Override
        public boolean supportsEdgeAddition() {
            return false;
        }

        @Override
        public boolean supportsEdgeRemoval() {
            return false;
        }

        @Override
        public boolean supportsEdgePropertyAddition() {
            return false;
        }

        @Override
        public boolean supportsEdgePropertyRemoval() {
            return false;
        }

        public <A> Class<A> getGraphWriter(org.apache.commons.configuration2.Configuration configuration) {
            try {
                return (Class) Class.forName(configuration.getString(Constants.GREMLIN_HADOOP_GRAPH_WRITER));
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
            if (janusGraph.configuration().containsKey(Constants.GREMLIN_HADOOP_GRAPH_WRITER)) {
                final Object writer = ReflectionUtils.newInstance(this.getGraphWriter(janusGraph.configuration()),
                    ConfUtil.makeHadoopConfiguration(janusGraph.configuration()));
                if (writer instanceof PersistResultGraphAware)
                    return ((PersistResultGraphAware) writer).supportsResultGraphPersistCombination(resultGraph, persist);
                else {
                    logger.warn(writer.getClass() + " does not implement " + PersistResultGraphAware.class.getSimpleName() + " and thus, persistence options are unknown -- assuming all options are possible");
                    return true;
                }
            } else {
                logger.warn("No " + Constants.GREMLIN_HADOOP_GRAPH_WRITER + " property provided and thus, persistence options are unknown -- assuming all options are possible");
                return true;
            }
        }

        @Override
        public boolean supportsDirectObjects() {
            return false;
        }
    }

    //////////

    public static File copyDirectoryIfNonExistent(final FileSystem fileSystem, final String directory) {
        try {
            final String hadoopGremlinLibsRemote = "hadoop-gremlin-" + Gremlin.version() + "-libs";
            final Path path = new Path(directory);
            if (Boolean.valueOf(System.getProperty("is.testing", "false")) || (fileSystem.exists(path) && fileSystem.isDirectory(path))) {
                final File tempDirectory = new File(System.getProperty("java.io.tmpdir"), hadoopGremlinLibsRemote);

                assert tempDirectory.exists() || tempDirectory.mkdirs();

                final Path tempPath = new Path(new Path(tempDirectory.toURI()), path.getName());

                final RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, false);
                while (files.hasNext()) {
                    final LocatedFileStatus f = files.next();
                    fileSystem.copyToLocalFile(false, f.getPath(), new Path(tempPath, f.getPath().getName()), true);
                }
                return new File(tempPath.toUri());
            } else
                return new File(directory);
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}

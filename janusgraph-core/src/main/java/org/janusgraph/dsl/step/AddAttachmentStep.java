package org.janusgraph.dsl.step;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.graphdb.vertices.AbstractVertex;
import org.janusgraph.kydsj.serialize.MediaData;

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddAttachmentStep<S extends Element> extends SideEffectStep<S>
    implements Mutating<Event.ElementPropertyChangedEvent>, TraversalParent, Scoping {

    private Parameters parameters = new Parameters();
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;
    public static final String KEY="attachment";

    public AddAttachmentStep(final Traversal.Admin traversal, final MediaData mediaData) {
        super(traversal);
        this.parameters.set(this,KEY, mediaData);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        List<MediaData> mediaDatas = this.parameters.get(traverser, KEY, () -> {
            return new MediaData("");
        });
        final Element element = traverser.get();
        if(element instanceof AbstractVertex){
            AbstractVertex vertex=(AbstractVertex)element;
            for(MediaData mediaData:mediaDatas){
                if(mediaData!=null&& StringUtils.isNotBlank(mediaData.getKey())) {
                    vertex.attachment(mediaData);
                }
            }
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public int hashCode() {
        final int hash = super.hashCode() ^ this.parameters.hashCode();
        return hash;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters);
    }

    @Override
    public AddAttachmentStep<S> clone() {
        final AddAttachmentStep<S> clone = (AddAttachmentStep<S>) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }
}
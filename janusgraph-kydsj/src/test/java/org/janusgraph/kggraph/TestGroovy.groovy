package org.janusgraph.kggraph

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Test

class TestGroovy extends AbstractKGgraphTest{

    @Test
    def ss(){
        def list = g.V().hasLabel('organization_qydw').has('organization_gsmc', '天津南大通用数据技术股份有限公司').repeat(__.in('link_gs').has('bl', gt(0.25)).in('link_hold')).until(loops().is(eq(5))).simplePath().toList()
        for(path in list){
            print(path)
        }
        return;
    }

    def gremlin(){
        def companyName = '天津南大通用数据技术股份有限公司'

        def findBeneficialOwners = { currentVertex ->
            currentVertex
                .in('link_gs').has('bl', P.gt(25))
                .out('link_hold')
        }

        def enterprise = g.V().hasLabel('organization_qydw').has('organization_gsmc', companyName).next()

        def holdingPaths = []

        while (enterprise) {
            def beneficialOwners = findBeneficialOwners(enterprise).toList()
            if (!beneficialOwners.isEmpty()) {
                def owner = beneficialOwners[0]
                def holdingPercentage = owner.value('bl')
                holdingPaths.add("${owner.value('label')}[${holdingPercentage}]")
                enterprise = owner
            } else {
                break
            }
        }

        for (path in holdingPaths) {
            println(path)
        }
    }
}

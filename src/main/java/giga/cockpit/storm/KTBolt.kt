package giga.cockpit.storm

import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

/**
 * Created by thesebas on 2016-02-26.
 */
class KTBolt() : BaseRichBolt() {

    private lateinit var collector: OutputCollector

    override fun prepare(map: MutableMap<Any?, Any?>, topologyContext: TopologyContext, outputCollector: OutputCollector) {
        collector = outputCollector
    }

    override fun execute(tuple: Tuple) {
        collector.emit(Values(tuple.getStringByField("url"), 123))
    }

    override fun declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer) {
        outputFieldsDeclarer.declare(Fields("url", "val"))
    }


}
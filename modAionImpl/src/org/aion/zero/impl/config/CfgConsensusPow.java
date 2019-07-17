package org.aion.zero.impl.config;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import org.aion.mcf.config.Cfg;
import org.aion.mcf.config.CfgConsensus;
import org.aion.util.types.AddressUtils;

public final class CfgConsensusPow extends CfgConsensus {

    private final CfgEnergyStrategy cfgEnergyStrategy;

    CfgConsensusPow() {
        this.mining = false;
        staking = false;
        this.minerAddress = AddressUtils.ZERO_ADDRESS.toString();
        stakerAddress = AddressUtils.ZERO_ADDRESS.toString();
        this.cpuMineThreads =
                (byte)
                        (Runtime.getRuntime().availableProcessors()
                                >> 1); // half the available processors
        this.extraData = "AION";
        this.cfgEnergyStrategy = new CfgEnergyStrategy();
        this.seed = false;
    }

    private boolean mining;

    //TODO: [unity] separate the stake settings
    private boolean staking;

    private boolean seed;

    private String minerAddress;

    private String stakerAddress;

    private byte cpuMineThreads;

    protected String extraData;

    public void fromXML(final XMLStreamReader sr) throws XMLStreamException {
        loop:
        while (sr.hasNext()) {
            int eventType = sr.next();
            switch (eventType) {
                case XMLStreamReader.START_ELEMENT:
                    String elementName = sr.getLocalName().toLowerCase();
                    switch (elementName) {
                        case "mining":
                            this.mining = Boolean.parseBoolean(Cfg.readValue(sr));
                            break;
                        case "staking":
                            staking = Boolean.parseBoolean(Cfg.readValue(sr));
                        case "seed":
                            this.seed = Boolean.parseBoolean(Cfg.readValue(sr));
                            break;
                        case "miner-address":
                            this.minerAddress = Cfg.readValue(sr);
                            break;
                        case "staker-address":
                            stakerAddress = Cfg.readValue(sr);
                            break;
                        case "cpu-mine-threads":
                            this.cpuMineThreads = Byte.valueOf(Cfg.readValue(sr));
                            break;
                        case "extra-data":
                            this.extraData = Cfg.readValue(sr);
                            break;
                        case "nrg-strategy":
                            this.cfgEnergyStrategy.fromXML(sr);
                            break;
                        default:
                            Cfg.skipElement(sr);
                            break;
                    }
                    break;
                case XMLStreamReader.END_ELEMENT:
                    break loop;
            }
        }
    }

    String toXML() {
        final XMLOutputFactory output = XMLOutputFactory.newInstance();
        output.setProperty("escapeCharacters", false);
        XMLStreamWriter xmlWriter;
        String xml;
        try {
            Writer strWriter = new StringWriter();
            xmlWriter = output.createXMLStreamWriter(strWriter);
            xmlWriter.writeCharacters("\r\n\t");
            xmlWriter.writeStartElement("consensus");

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("mining");
            xmlWriter.writeCharacters(this.getMining() + "");
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("staking");
            xmlWriter.writeCharacters(getStaking() + "");
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("miner-address");
            xmlWriter.writeCharacters(this.getMinerAddress());
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("staker-address");
            xmlWriter.writeCharacters(getStakerAddress());
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("cpu-mine-threads");
            xmlWriter.writeCharacters(this.getCpuMineThreads() + "");
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("extra-data");
            xmlWriter.writeCharacters(this.getExtraData());
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeStartElement("nrg-strategy");
            xmlWriter.writeCharacters(this.cfgEnergyStrategy.toXML());
            xmlWriter.writeCharacters("\r\n\t\t");
            xmlWriter.writeEndElement();

            xmlWriter.writeCharacters("\r\n\t");
            xmlWriter.writeEndElement();
            xml = strWriter.toString();
            strWriter.flush();
            strWriter.close();
            xmlWriter.flush();
            xmlWriter.close();
            return xml;
        } catch (IOException | XMLStreamException e) {
            e.printStackTrace();
            return "";
        }
    }

    public void setExtraData(final String _extraData) {
        this.extraData = _extraData;
    }

    public void setMining(final boolean value) {
        this.mining = value;
    }

    public boolean getMining() {
        return this.mining;
    }

    public boolean getStaking() {
        return staking;
    }

    public String getStakerAddress() {
        return stakerAddress;
    }

    public byte getCpuMineThreads() {
        int procs = Runtime.getRuntime().availableProcessors();
        return (byte) Math.min(procs, this.cpuMineThreads);
    }

    public String getExtraData() {
        return this.extraData;
    }

    public String getMinerAddress() {
        return this.minerAddress;
    }

    public CfgEnergyStrategy getEnergyStrategy() {
        return this.cfgEnergyStrategy;
    }

    public boolean isSeed() {
        return seed;
    }

    @VisibleForTesting
    public void setSeed(final boolean value) {
        seed = value;
    }
}

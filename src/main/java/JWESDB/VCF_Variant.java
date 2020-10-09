package JWESDB;

import java.util.List;

public class VCF_Variant {
	
	String chrom_number = "NA";
	Long chrom_position = 0L;
	String chrom_id = "NA";
	String ref_base = "NA";
	String alt_base = "NA";
	float quality = 0;
	String filter = "NA";
	String description = "NA";
	VCF_Info info = null;
	List<VCF_Sample> samples = null;
	
	
	public String getChrom_number() {
		return chrom_number;
	}
	public void setChrom_number(String chrom_number) {
		this.chrom_number = chrom_number;
	}
	public Long getChrom_position() {
		return chrom_position;
	}
	public void setChrom_position(Long chrom_position) {
		this.chrom_position = chrom_position;
	}
	public String getChrom_id() {
		return chrom_id;
	}
	public void setChrom_id(String chrom_id) {
		this.chrom_id = chrom_id;
	}
	public String getRef_base() {
		return ref_base;
	}
	public void setRef_base(String ref_base) {
		this.ref_base = ref_base;
	}
	public String getAlt_base() {
		return alt_base;
	}
	public void setAlt_base(String alt_base) {
		this.alt_base = alt_base;
	}
	public float getQuality() {
		return quality;
	}
	public void setQuality(float quality) {
		this.quality = quality;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public VCF_Info getInfo() {
		return info;
	}
	public void setInfo(VCF_Info info) {
		this.info = info;
	}
	public List<VCF_Sample> getFormat() {
		return samples;
	}
	public void setFormat(List<VCF_Sample> format) {
		this.samples = format;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public List<VCF_Sample> getSamples() {
		return samples;
	}
	public void setSamples(List<VCF_Sample> samples) {
		this.samples = samples;
	}
	
}

package org.java.plus.dag.core.base.utils.metrics;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.SigarNotImplementedException;
import org.hyperic.sigar.Swap;

import java.net.InetAddress;
import java.net.UnknownHostException;

@SuppressWarnings("all")
public class SystemInfo {
    // 1.CPU��Դ��Ϣ // a)CPU��������λ������
    public static int getCpuCount() throws SigarException {
        Sigar sigar = new Sigar();
        try {
            return sigar.getCpuInfoList().length;
        } finally {
            sigar.close();
        }
    }

    // b)CPU����������λ��HZ����CPU�������Ϣ
    public static void getCpuTotal() {
        Sigar sigar = new Sigar();
        CpuInfo[] infos;
        try {
            infos = sigar.getCpuInfoList();
            for (CpuInfo info : infos) {// �����ǵ���CPU���Ƕ�CPU������
                System.out.println("mhz=" + info.getMhz());// CPU������MHz
                System.out.println("vendor=" + info.getVendor());// ���CPU���������磺Intel
                System.out.println("model=" + info.getModel());// ���CPU������磺Celeron
                System.out.println("cache size=" + info.getCacheSize());// ����洢������
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }

    // c)CPU���û�ʹ������ϵͳʹ��ʣ�������ܵ�ʣ�������ܵ�ʹ��ռ�����ȣ���λ��100%��
    public static void testCpuPerc() {
        Sigar sigar = new Sigar();
        // ��ʽһ����Ҫ�����һ��CPU�����
        CpuPerc cpu;
        try {
            cpu = sigar.getCpuPerc();
            printCpuPerc(cpu);
        } catch (SigarException e) {
            e.printStackTrace();
        }
        // ��ʽ���������ǵ���CPU���Ƕ�CPU������
        CpuPerc[] cpuList = null;
        try {
            cpuList = sigar.getCpuPercList();
        } catch (SigarException e) {
            e.printStackTrace();
            return;
        }
        for (CpuPerc cpuPerc : cpuList) {
            printCpuPerc(cpuPerc);
        }
    }

    private static void printCpuPerc(CpuPerc cpu) {
        System.out.println("User :" + CpuPerc.format(cpu.getUser()));// �û�ʹ����
        System.out.println("Sys :" + CpuPerc.format(cpu.getSys()));// ϵͳʹ����
        System.out.println("Wait :" + CpuPerc.format(cpu.getWait()));// ��ǰ�ȴ���
        System.out.println("Nice :" + CpuPerc.format(cpu.getNice()));//
        System.out.println("Idle :" + CpuPerc.format(cpu.getIdle()));// ��ǰ������
        System.out.println("Total :" + CpuPerc.format(cpu.getCombined()));// �ܵ�ʹ����
    }

    // 2.�ڴ���Դ��Ϣ
    public static void getPhysicalMemory() {
        // a)�����ڴ���Ϣ
        Sigar sigar = new Sigar();
        Mem mem;
        try {
            mem = sigar.getMem();
            // �ڴ�����
            System.out.println("Total = " + mem.getTotal() / 1024L + "K av");
            // ��ǰ�ڴ�ʹ����
            System.out.println("Used = " + mem.getUsed() / 1024L + "K used");
            // ��ǰ�ڴ�ʣ����
            System.out.println("Free = " + mem.getFree() / 1024L + "K free");  // b)ϵͳҳ���ļ���������Ϣ
            Swap swap = sigar.getSwap();
            // ����������
            System.out.println("Total = " + swap.getTotal() / 1024L + "K av");
            // ��ǰ������ʹ����
            System.out.println("Used = " + swap.getUsed() / 1024L + "K used");
            // ��ǰ������ʣ����
            System.out.println("Free = " + swap.getFree() / 1024L + "K free");
        } catch (SigarException e) {
            e.printStackTrace();
        }
    } // 3.����ϵͳ��Ϣ // a)ȡ����ǰ����ϵͳ�����ƣ�

    public static String getPlatformName() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception exc) {
            Sigar sigar = new Sigar();
            try {
                hostname = sigar.getNetInfo().getHostName();
            } catch (SigarException e) {
                hostname = "localhost.unknown";
            } finally {
                sigar.close();
            }
        }
        return hostname;
    }

    // b)ȡ��ǰ����ϵͳ����Ϣ
    public static void testGetOSInfo() {
        OperatingSystem OS = OperatingSystem.getInstance();
        // ����ϵͳ�ں������磺 386��486��586��x86
        System.out.println("OS.getArch() = " + OS.getArch());
        System.out.println("OS.getCpuEndian() = " + OS.getCpuEndian());//
        System.out.println("OS.getDataModel() = " + OS.getDataModel());//
        // ϵͳ����
        System.out.println("OS.getDescription() = " + OS.getDescription());
        System.out.println("OS.getMachine() = " + OS.getMachine());//
        // ����ϵͳ����
        System.out.println("OS.getName() = " + OS.getName());
        System.out.println("OS.getPatchLevel() = " + OS.getPatchLevel());//
        // ����ϵͳ������
        System.out.println("OS.getVendor() = " + OS.getVendor());
        // ��������
        System.out.println("OS.getVendorCodeName() = " + OS.getVendorCodeName());
        // ����ϵͳ����
        System.out.println("OS.getVendorName() = " + OS.getVendorName());
        // ����ϵͳ��������
        System.out.println("OS.getVendorVersion() = " + OS.getVendorVersion());
        // ����ϵͳ�İ汾��
        System.out.println("OS.getVersion() = " + OS.getVersion());
    }

    // c)ȡ��ǰϵͳ���̱��е��û���Ϣ
    public static void testWho() {
        try {
            Sigar sigar = new Sigar();
            org.hyperic.sigar.Who[] who = sigar.getWhoList();
            if (who != null && who.length > 0) {
                for (int i = 0; i < who.length; i++) {
                    System.out.println("\n~~~~~~~~~" + String.valueOf(i) + "~~~~~~~~~");
                    org.hyperic.sigar.Who _who = who[i];
                    System.out.println("getDevice() = " + _who.getDevice());
                    System.out.println("getHost() = " + _who.getHost());
                    System.out.println("getTime() = " + _who.getTime());
                    // ��ǰϵͳ���̱��е��û���
                    System.out.println("getUser() = " + _who.getUser());
                }
            }
        } catch (SigarException e) {
            e.printStackTrace();
        }
    }

    // 4.��Դ��Ϣ����Ҫ��Ӳ�̣� // a)ȡӲ�����еķ���������ϸ��Ϣ��ͨ��sigar.getFileSystemList()�����FileSystem�б����Ȼ�������б�������
    public static void testFileSystemInfo() throws Exception {
        Sigar sigar = new Sigar();
        FileSystem[] fslist = sigar.getFileSystemList();
        //String dir = System.getProperty("user.home");// ��ǰ�û��ļ���·��
        for (int i = 0; i < fslist.length; i++) {
            System.out.println("\n~~~~~~~~~~" + i + "~~~~~~~~~~");
            FileSystem fs = fslist[i];
            // �������̷�����
            System.out.println("fs.getDevName() = " + fs.getDevName());
            // �������̷�����
            System.out.println("fs.getDirName() = " + fs.getDirName());
            System.out.println("fs.getFlags() = " + fs.getFlags());//
            // �ļ�ϵͳ���ͣ����� FAT32��NTFS
            System.out.println("fs.getSysTypeName() = " + fs.getSysTypeName());
            // �ļ�ϵͳ�����������籾��Ӳ�̡������������ļ�ϵͳ��
            System.out.println("fs.getTypeName() = " + fs.getTypeName());
            // �ļ�ϵͳ����
            System.out.println("fs.getType() = " + fs.getType());
            FileSystemUsage usage = null;
            try {
                usage = sigar.getFileSystemUsage(fs.getDirName());
            } catch (SigarException e) {
                if (fs.getType() == 2) {
                    throw e;
                }
                continue;
            }
            switch (fs.getType()) {
                case 0: // TYPE_UNKNOWN ��δ֪
                    break;
                case 1: // TYPE_NONE
                    break;
                case 2: // TYPE_LOCAL_DISK : ����Ӳ��
                    // �ļ�ϵͳ�ܴ�С
                    System.out.println(" Total = " + usage.getTotal() + "KB");
                    // �ļ�ϵͳʣ���С
                    System.out.println(" Free = " + usage.getFree() + "KB");
                    // �ļ�ϵͳ���ô�С
                    System.out.println(" Avail = " + usage.getAvail() + "KB");
                    // �ļ�ϵͳ�Ѿ�ʹ����
                    System.out.println(" Used = " + usage.getUsed() + "KB");
                    double usePercent = usage.getUsePercent() * 100D;
                    // �ļ�ϵͳ��Դ��������
                    System.out.println(" Usage = " + usePercent + "%");
                    break;
                case 3:// TYPE_NETWORK ������
                    break;
                case 4:// TYPE_RAM_DISK ������
                    break;
                case 5:// TYPE_CDROM ������
                    break;
                case 6:// TYPE_SWAP ��ҳ�潻��
                    break;
            }
            System.out.println(" DiskReads = " + usage.getDiskReads());
            System.out.println(" DiskWrites = " + usage.getDiskWrites());
        }
    }

    // 5.������Ϣ // a)��ǰ��������ʽ����
    public static String getFQDN() {
        Sigar sigar = null;
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            try {
                sigar = new Sigar();
                return sigar.getFQDN();
            } catch (SigarException ex) {
                return null;
            } finally {
                if (sigar != null) {
                    sigar.close();
                }
            }
        }
    } // b)ȡ����ǰ������IP��ַ

    public static String getDefaultIpAddress() {
        String address = null;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
            // û�г����쳣��������ȡ����IPʱ�����ȡ���Ĳ�������ѭ�ص�ַʱ�ͷ���
            // ������ͨ��Sigar���߰��еķ�������ȡ
            if (!NetFlags.LOOPBACK_ADDRESS.equals(address)) {
                return address;
            }
        } catch (UnknownHostException e) {
            // hostname not in DNS or /etc/hosts
        }
        Sigar sigar = new Sigar();
        try {
            address = sigar.getNetInterfaceConfig().getAddress();
        } catch (SigarException e) {
            address = NetFlags.LOOPBACK_ADDRESS;
        } finally {
            sigar.close();
        }
        return address;
    } // c)ȡ����ǰ������MAC��ַ

    public static String getMAC() {
        Sigar sigar = null;
        try {
            sigar = new Sigar();
            String[] ifaces = sigar.getNetInterfaceList();
            String hwaddr = null;
            for (String iface : ifaces) {
                NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(iface);
                if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress()) || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0 || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr())) {
                    continue;
                }
                /*
                 * ������ڶ������������������������Ĭ��ֻȡ��һ��������MAC��ַ�����Ҫ�������е���������������ĺ�����ģ�������޸ķ����ķ�������Ϊ�����Collection
                 * ��ͨ����forѭ����ȡ���Ķ��MAC��ַ��
                 */
                hwaddr = cfg.getHwaddr();
                break;
            }
            return hwaddr;
        } catch (Exception e) {
            return null;
        } finally {
            if (sigar != null) {
                sigar.close();
            }
        }
    }

    // d)��ȡ������������Ϣ
    public static void testNetIfList() throws Exception {
        Sigar sigar = new Sigar();
        String[] ifNames = sigar.getNetInterfaceList();
        for (String name : ifNames) {
            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
            print("\nname = " + name);// �����豸��
            print("Address = " + ifconfig.getAddress());// IP��ַ
            print("Netmask = " + ifconfig.getNetmask());// ��������
            if ((ifconfig.getFlags() & 1L) <= 0L) {
                print("!IFF_UP...skipping getNetInterfaceStat");
                continue;
            }
            try {
                NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);
                print("RxPackets = " + ifstat.getRxPackets());// ���յ��ܰ�����
                print("TxPackets = " + ifstat.getTxPackets());// ���͵��ܰ�����
                print("RxBytes = " + ifstat.getRxBytes());// ���յ������ֽ���
                print("TxBytes = " + ifstat.getTxBytes());// ���͵����ֽ���
                print("RxErrors = " + ifstat.getRxErrors());// ���յ��Ĵ������
                print("TxErrors = " + ifstat.getTxErrors());// �������ݰ�ʱ�Ĵ�����
                print("RxDropped = " + ifstat.getRxDropped());// ����ʱ�����İ���
                print("TxDropped = " + ifstat.getTxDropped());// ����ʱ�����İ���
            } catch (SigarNotImplementedException ignored) {
            } catch (SigarException e) {
                print(e.getMessage());
            }
        }
    }

    static void print(String msg) {
        System.out.println(msg);
    }

    // e)һЩ��������Ϣ
    public static void getEthernetInfo() {
        Sigar sigar = null;
        try {
            sigar = new Sigar();
            String[] ifaces = sigar.getNetInterfaceList();
            for (String iface : ifaces) {
                NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(iface);
                if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress()) || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0 || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr())) {
                    continue;
                }
                System.out.println("cfg.getAddress() = " + cfg.getAddress());// IP��ַ
                System.out.println("cfg.getBroadcast() = " + cfg.getBroadcast());// ���ع㲥��ַ
                System.out.println("cfg.getHwaddr() = " + cfg.getHwaddr());// ����MAC��ַ
                System.out.println("cfg.getNetmask() = " + cfg.getNetmask());// ��������
                System.out.println("cfg.getDescription() = " + cfg.getDescription());// ����������Ϣ
                System.out.println("cfg.getType() = " + cfg.getType());//
                System.out.println("cfg.getDestination() = " + cfg.getDestination());
                System.out.println("cfg.getFlags() = " + cfg.getFlags());//
                System.out.println("cfg.getMetric() = " + cfg.getMetric());
                System.out.println("cfg.getMtu() = " + cfg.getMtu());
                System.out.println("cfg.getName() = " + cfg.getName());
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Error while creating GUID" + e);
        } finally {
            if (sigar != null) {
                sigar.close();
            }
        }
    }

//    public static void main(String[] args) throws SigarException {
//        SystemInfo s = new SystemInfo();
//		s.getCpuTotal();
//		s.getEthernetInfo();
//		s.getDefaultIpAddress();
//        s.testGetOSInfo();
//		s.getPhysicalMemory();
//        System.out.println(getCpuCount());
//    }
}
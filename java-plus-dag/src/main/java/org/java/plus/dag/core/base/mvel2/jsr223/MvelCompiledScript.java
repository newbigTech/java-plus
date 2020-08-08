package org.java.plus.dag.core.base.mvel2.jsr223;

import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import java.io.Serializable;

public class MvelCompiledScript extends CompiledScript {

    private final MvelScriptEngine scriptEngine;
    private final Serializable compiledScript;

    public MvelCompiledScript(MvelScriptEngine engine, Serializable compiledScript) {
        this.scriptEngine = engine;
        this.compiledScript = compiledScript;
    }

    @Override
    public Object eval(ScriptContext context) throws ScriptException {
        return scriptEngine.evaluate(compiledScript, context);
    }

    @Override
    public ScriptEngine getEngine() {
        return scriptEngine;
    }
}

import reggae;
enum commonFlags = "-w -g -debug";
mixin build!(dubTestTarget!(CompilerFlags(commonFlags)));
